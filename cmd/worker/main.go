package main

import (
	"context"
	"database/sql"
	"log"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"

	"my-go-service/internal/config"
	"my-go-service/internal/consumer"
	"my-go-service/internal/events"
	"my-go-service/internal/repository/postgres"
)

const (
	topic       = "order-events"
	consumerGrp = "order-events-logger"
)

func main() {
	cfg := config.Load()

	// --- Postgres ---
	db, err := sql.Open("postgres", cfg.PostgresDSN())
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := db.PingContext(ctx); err != nil {
		cancel()
		log.Fatalf("db ping: %v", err)
	}
	cancel()
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
	defer reader.Close()
	log.Printf("Kafka reader ready: topic=%s group=%s brokers=%s",
		topic, consumerGrp, cfg.KafkaBrokers)

	// --- Consumer ---

	dlqProducer := events.NewDLQProducer(cfg.KafkaBrokers, "order-events-dlq")
	defer dlqProducer.Close()
	log.Println("DLQ producer ready: topic=order-events-dlq")

	eventRepo := postgres.NewEventLogRepo(db)
	ordersRead := postgres.NewOrdersReadRepo(db) // ← новое
	orderConsumer := consumer.NewOrderConsumer(reader, eventRepo, ordersRead, dlqProducer, db)

	// --- Graceful shutdown ---
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := orderConsumer.Run(ctx); err != nil {
		log.Fatalf("consumer error: %v", err)
	}
	log.Println("worker stopped gracefully")
}
