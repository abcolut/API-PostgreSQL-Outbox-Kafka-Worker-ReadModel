package consumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/segmentio/kafka-go"

	"my-go-service/internal/domain"
	"my-go-service/internal/events"
	"my-go-service/internal/repository/postgres"
)

type OrderConsumer struct {
	reader         *kafka.Reader
	eventRepo      *postgres.EventLogRepo
	ordersReadRepo *postgres.OrdersReadRepo
	dlq            *events.DLQProducer
	db             *sql.DB
}

func NewOrderConsumer(
	reader *kafka.Reader,
	eventRepo *postgres.EventLogRepo,
	ordersReadRepo *postgres.OrdersReadRepo,
	dlq *events.DLQProducer,
	db *sql.DB,
) *OrderConsumer {
	return &OrderConsumer{
		reader:         reader,
		eventRepo:      eventRepo,
		ordersReadRepo: ordersReadRepo,
		dlq:            dlq,
		db:             db,
	}
}

// Run блокируется, пока не отменят ctx.
func (c *OrderConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			log.Printf("read error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		if err := c.processMessage(ctx, msg); err != nil {
			log.Printf("process error (offset NOT committed): partition=%d offset=%d err=%v",
				msg.Partition, msg.Offset, err)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit error: %v", err)
		}
	}
}

func (c *OrderConsumer) processMessage(ctx context.Context, msg kafka.Message) error {
	var evt events.OrderEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		if dlqErr := c.dlq.Send(ctx, msg, events.ReasonDecodeError, err.Error()); dlqErr != nil {
			return dlqErr
		}
		log.Printf("bad message sent to DLQ: partition=%d offset=%d err=%v",
			msg.Partition, msg.Offset, err)
		return nil
	}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 1. Идемпотентность через order_events_log
	inserted, err := c.insertEventTx(ctx, tx, msg, evt)
	if err != nil {
		return err
	}
	if !inserted {
		log.Printf("event already processed (skip): partition=%d offset=%d",
			msg.Partition, msg.Offset)
		return nil
	}

	// 2. Применяем к read-модели
	switch evt.Type {
	case "OrderCreated", "OrderUpdated":
		order, ok := extractOrder(evt.Payload)
		if !ok {
			if dlqErr := c.dlq.Send(ctx, msg, "bad_payload", "cannot parse order"); dlqErr != nil {
				return dlqErr
			}
			return nil
		}
		if err := c.ordersReadRepo.Upsert(ctx, tx, order, false); err != nil {
			return err
		}
	case "OrderDeleted":
		if _, err := tx.ExecContext(ctx, `
			UPDATE orders_read
			SET deleted = TRUE, last_event_at = NOW(),
			    event_count = event_count + 1
			WHERE id = $1
		`, evt.OrderID); err != nil {
			return err
		}
	default:
		log.Printf("unknown event type: %s", evt.Type)
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	log.Printf("event applied: type=%s order_id=%d partition=%d offset=%d",
		evt.Type, evt.OrderID, msg.Partition, msg.Offset)
	return nil
}

// insertEventTx — идемпотентная запись в order_events_log.
// Возвращает false, если событие уже было обработано ранее.
func (c *OrderConsumer) insertEventTx(ctx context.Context, tx *sql.Tx, msg kafka.Message, evt events.OrderEvent) (bool, error) {
	var id int64
	err := tx.QueryRowContext(ctx, `
		INSERT INTO order_events_log
			(event_type, order_id, payload, kafka_topic, kafka_partition, kafka_offset)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
		RETURNING id
	`, evt.Type, evt.OrderID, msg.Value, msg.Topic, msg.Partition, msg.Offset).Scan(&id)

	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func extractOrder(payload any) (domain.Order, bool) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return domain.Order{}, false
	}
	var o domain.Order
	if err := json.Unmarshal(raw, &o); err != nil {
		return domain.Order{}, false
	}
	return o, true
}
