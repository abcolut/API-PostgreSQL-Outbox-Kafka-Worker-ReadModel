package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type OutboxRepo struct {
	db *sql.DB
}

func NewOutboxRepo(db *sql.DB) *OutboxRepo {
	return &OutboxRepo{db: db}
}

type OutboxEntry struct {
	ID          int64
	AggregateID int64
	EventType   string
	Payload     json.RawMessage
	Status      string
	Attempts    int
}

// InsertTx пишет событие в outbox В ТОЙ ЖЕ транзакции, что и бизнес-данные.
func (r *OutboxRepo) InsertTx(ctx context.Context, tx *sql.Tx, aggregateID int64, eventType string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO outbox (aggregate_id, event_type, payload)
		VALUES ($1, $2, $3)
	`, aggregateID, eventType, raw)
	return err
}

// ClaimBatch захватывает пачку pending-записей для отправки.
// FOR UPDATE SKIP LOCKED — ключ к безопасной работе нескольких relay-процессов.
func (r *OutboxRepo) ClaimBatch(ctx context.Context, limit int) ([]OutboxEntry, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT id, aggregate_id, event_type, payload, status, attempts
		FROM outbox
		WHERE status = 'pending'
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []OutboxEntry
	for rows.Next() {
		var e OutboxEntry
		if err := rows.Scan(&e.ID, &e.AggregateID, &e.EventType, &e.Payload, &e.Status, &e.Attempts); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return entries, tx.Commit()
}

// MarkSent помечает запись отправленной.
func (r *OutboxRepo) MarkSent(ctx context.Context, id int64) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE outbox SET status = 'sent', sent_at = NOW(), attempts = attempts + 1
		WHERE id = $1
	`, id)
	return err
}

// MarkFailed увеличивает счётчик попыток и сохраняет ошибку.
func (r *OutboxRepo) MarkFailed(ctx context.Context, id int64, errMsg string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE outbox
		SET attempts = attempts + 1, last_error = $2,
		    status = CASE WHEN attempts + 1 >= 10 THEN 'failed' ELSE 'pending' END
		WHERE id = $1
	`, id, errMsg)
	return err
}

// DeleteSentOlderThan удаляет sent-записи старше заданного интервала.
// Возвращает количество удалённых строк.
func (r *OutboxRepo) DeleteSentOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	res, err := r.db.ExecContext(ctx, `
		DELETE FROM outbox
		WHERE status = 'sent'
		  AND sent_at < NOW() - $1::interval
	`, fmt.Sprintf("%d seconds", int(age.Seconds())))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// SetFailedFlagToPending превращает проблемные строки в норамальные
// Возвращает количество превращенных строк.
func (r *OutboxRepo) SetFailedFlagToPending(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE outbox SET status = 'pending', attempts = 0 WHERE status = 'failed';
	`)
	return err

}
