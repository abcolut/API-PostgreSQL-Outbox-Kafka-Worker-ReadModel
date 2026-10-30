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
	"my-go-service/internal/repository/postgres"
)

const (
	pollInterval = 500 * time.Millisecond
	batchSize    = 50
)

func healtRelayhHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status_relay":"ok"}`))
}

func main() {
	cfg := config.Load()

	db, err := sql.Open("postgres", cfg.PostgresDSN())
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers),
		Topic:        "order-events",
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
	}
	// Не закрываем сразу: закроем при остановке сервиса
	// defer writer.Close() — убираем отсюда, чтобы закрыть в Shutdown

	outboxRepo := postgres.NewOutboxRepo(db)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Println("relay started")

	outboxRepo.SetFailedFlagToPending(ctx)

	// Периодическая очистка outbox
	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Println("cleaner stopped")
				return
			case <-ticker.C:
				deleted, err := outboxRepo.DeleteSentOlderThan(ctx, 24*time.Hour)
				if err != nil {
					log.Printf("outbox cleanup error: %v", err)
				} else {
					log.Printf("outbox cleanup: deleted %d rows", deleted)
				}
			}
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healtRelayhHandler)

	c := cors.New(cors.Options{
		AllowedOrigins:   []string{"http://localhost:8080"},
		AllowCredentials: true,
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Content-Type", "Authorization"},
	})

	handler := c.Handler(mux)

	srv := &http.Server{
		Addr:    ":8081",
		Handler: handler,
	}

	log.Println("Server starting on :8081")

	// Запускаем сервер в горутине, чтобы не блокировать main
	go func() {
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Основной цикл обработки outbox
	for {
		select {
		case <-ctx.Done():
			log.Println("shutdown requested")

			// Сначала останавливаем HTTP‑сервер
			if err := srv.Shutdown(ctx); err != nil {
				log.Printf("HTTP shutdown error: %v", err)
			}

			// Закрываем Kafka writer
			if closeErr := writer.Close(); closeErr != nil {
				log.Printf("Kafka writer close error: %v", closeErr)
			}

			log.Println("relay stopped")
			return
		default:
			entries, err := outboxRepo.ClaimBatch(ctx, batchSize)
			if err != nil {
				log.Printf("claim batch: %v", err)
				time.Sleep(time.Second)
				continue
			}
			if len(entries) == 0 {
				time.Sleep(pollInterval)
				continue
			}

			for _, e := range entries {
				msg := kafka.Message{
					Key:   []byte(formatKey(e.AggregateID)),
					Value: e.Payload,
				}
				if writeErr := writer.WriteMessages(ctx, msg); writeErr != nil {
					log.Printf("publish id=%d: %v", e.ID, writeErr)
					outboxRepo.MarkFailed(ctx, e.ID, writeErr.Error())
					continue
				}
				if markErr := outboxRepo.MarkSent(ctx, e.ID); markErr != nil {
					log.Printf("mark sent id=%d: %v", e.ID, markErr)
				}
				log.Printf("published outbox id=%d type=%s order_id=%d", e.ID, e.EventType, e.AggregateID)
			}
		}
	}
}

func formatKey(id int64) string {
	return "order-" + itoa(id)
}

func itoa(id int64) string {
	if id == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for id > 0 {
		i--
		buf[i] = byte('0' + id%10)
		id /= 10
	}
	return string(buf[i:])
}
