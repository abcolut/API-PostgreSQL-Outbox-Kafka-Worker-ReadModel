package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"

	"github.com/rs/cors"
	"my-go-service/internal/config"
	"my-go-service/internal/consumer"
	"my-go-service/internal/events"
	"my-go-service/internal/repository/postgres"
)

const (
	topic       = "order-events"
	consumerGrp = "order-events-logger"
)

func healtWorkerHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status_worker":"ok"}`))
}

func main() {
	cfg := config.Load()

	// --- Postgres ---
	db, err := sql.Open("postgres", cfg.PostgresDSN())
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	// Проверка соединения (одноразовая, не нужно держать отдельный ctx)
	if err := db.Ping(); err != nil {
		log.Fatalf("db ping: %v", err)
	}
	log.Println("PostgreSQL connected")

	// --- Kafka Reader ---
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{cfg.KafkaBrokers},
		GroupID:     consumerGrp,
		Topic:       topic,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafka.FirstOffset,
	})
	// Не закрываем сразу: закроем при остановке
	// defer reader.Close() — убираем отсюда

	dlqProducer := events.NewDLQProducer(cfg.KafkaBrokers, "order-events-dlq")
	// defer dlqProducer.Close() — тоже убираем

	eventRepo := postgres.NewEventLogRepo(db)
	ordersRead := postgres.NewOrdersReadRepo(db)
	orderConsumer := consumer.NewOrderConsumer(reader, eventRepo, ordersRead, dlqProducer, db)

	// --- Graceful shutdown ---
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healtWorkerHandler)

	c := cors.New(cors.Options{
		AllowedOrigins:   []string{"http://localhost:8080"},
		AllowCredentials: true,
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type", "Authorization"},
	})

	handler := c.Handler(mux)

	srv := &http.Server{
		Addr:    ":8082",
		Handler: handler,
	}

	log.Println("Server starting on :8082")

	// 1. HTTP сервер — в горутине
	go func() {
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err) // не Fatal, чтобы дальше шла остановка
		}
	}()

	// 2. Consumer — в горутине, чтобы не блокировать main
	var consumerErr error
	go func() {
		consumerErr = orderConsumer.Run(ctx)
	}()

	// 3. Главный цикл: ждём остановки и делаем Shutdown
	<-ctx.Done()
	log.Println("shutdown requested")

	// Останавливаем HTTP-сервер
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}
	log.Println("HTTP server stopped")

	// Ждём завершения consumer (если он ещё не упал с ошибкой)
	// Если Run уже вернул ошибку — она будет в consumerErr
	// Если ещё работает — ctx уже отменён, он должен корректно выйти
	if consumerErr != nil && consumerErr != context.Canceled {
		log.Printf("consumer error: %v", consumerErr)
	} else {
		log.Println("consumer stopped gracefully")
	}

	// Закрываем ресурсы в правильном порядке
	if closeErr := dlqProducer.Close(); closeErr != nil {
		log.Printf("DLQ producer close error: %v", closeErr)
	}
	if closeErr := reader.Close(); closeErr != nil {
		log.Printf("Kafka reader close error: %v", closeErr)
	}

	log.Println("service stopped")
}
