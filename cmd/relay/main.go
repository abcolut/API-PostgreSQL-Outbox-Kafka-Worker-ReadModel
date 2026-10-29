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
	"my-go-service/internal/repository/postgres"
)

const (
	pollInterval = 500 * time.Millisecond
	batchSize    = 50
)

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
	defer writer.Close()

	outboxRepo := postgres.NewOutboxRepo(db)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Println("relay started")

	//если сервис простаивал - то нужно попробовать разбудить зависшие заказы
	//Вообще говоря, с каждым Failed нужно разбираться, но в учебном проекте я сделал автоматический ресет состояния
	outboxRepo.SetFailedFlagToPending(ctx)

	// Периодическая очистка outbox (раз в час)
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
					continue
				}
				log.Printf("outbox cleanup: deleted %d rows", deleted)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Println("relay stopped")
			return
		default:
		}

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
			if err := writer.WriteMessages(ctx, msg); err != nil {
				log.Printf("publish id=%d: %v", e.ID, err)
				outboxRepo.MarkFailed(ctx, e.ID, err.Error())
				continue
			}
			if err := outboxRepo.MarkSent(ctx, e.ID); err != nil {
				log.Printf("mark sent id=%d: %v", e.ID, err)
			}
			log.Printf("published outbox id=%d type=%s order_id=%d", e.ID, e.EventType, e.AggregateID)
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
