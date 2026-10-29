package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/lib/pq"
)

type EventLogRepo struct {
	db *sql.DB
}

func NewEventLogRepo(db *sql.DB) *EventLogRepo {
	return &EventLogRepo{db: db}
}

type EventLogEntry struct {
	EventType      string
	OrderID        int64
	Payload        json.RawMessage
	KafkaTopic     string
	KafkaPartition int
	KafkaOffset    int64
}

// ErrAlreadyProcessed возвращается, если событие уже было записано
// (повторная доставка из Kafka).
var ErrAlreadyProcessed = errors.New("event already processed")

func (r *EventLogRepo) Insert(ctx context.Context, e EventLogEntry) error {
	const query = `
		INSERT INTO order_events_log
			(event_type, order_id, payload, kafka_topic, kafka_partition, kafka_offset)
		VALUES ($1, $2, $3, $4, $5, $6)
	`

	_, err := r.db.ExecContext(ctx, query,
		e.EventType, e.OrderID, e.Payload,
		e.KafkaTopic, e.KafkaPartition, e.KafkaOffset,
	)
	if err != nil {
		// pq.Error.Code == "23505" — unique_violation
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "23505" {
			return ErrAlreadyProcessed
		}
		return err
	}
	return nil
}
