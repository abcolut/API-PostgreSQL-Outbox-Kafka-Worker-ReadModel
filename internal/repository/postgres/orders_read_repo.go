package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"my-go-service/internal/domain"
)

type OrdersReadRepo struct {
	db *sql.DB
}

func NewOrdersReadRepo(db *sql.DB) *OrdersReadRepo {
	return &OrdersReadRepo{db: db}
}

// Upsert применяет событие к read-модели.
// Идемпотентен при повторной доставке, если payload содержит финальное состояние.
func (r *OrdersReadRepo) Upsert(ctx context.Context, tx *sql.Tx, o domain.Order, deleted bool) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO orders_read
			(id, user_id, item, quantity, status, created_at, updated_at,
			 last_event_at, event_count, deleted)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), 1, $8)
		ON CONFLICT (id) DO UPDATE SET
			user_id       = EXCLUDED.user_id,
			item          = EXCLUDED.item,
			quantity      = EXCLUDED.quantity,
			status        = EXCLUDED.status,
			updated_at    = EXCLUDED.updated_at,
			last_event_at = NOW(),
			event_count   = orders_read.event_count + 1,
			deleted       = EXCLUDED.deleted
	`,
		o.ID, o.UserID, o.Item, o.Quantity, o.Status, o.CreatedAt, o.UpdatedAt, deleted)
	return err
}

func (r *OrdersReadRepo) GetByID(ctx context.Context, id int64) (*domain.Order, error) {
	const query = `
		SELECT id, user_id, item, quantity, status, created_at, updated_at
		FROM orders_read
		WHERE id = $1 AND deleted = FALSE
	`
	var o domain.Order
	err := r.db.QueryRowContext(ctx, query, id).
		Scan(&o.ID, &o.UserID, &o.Item, &o.Quantity, &o.Status, &o.CreatedAt, &o.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &o, nil
}

func (r *OrdersReadRepo) List(ctx context.Context, f domain.OrderFilter) ([]domain.Order, error) {
	var (
		conds = []string{"deleted = FALSE"}
		args  []any
		i     = 1
	)

	if f.UserID != nil {
		conds = append(conds, fmt.Sprintf("user_id = $%d", i))
		args = append(args, *f.UserID)
		i++
	}
	if f.Status != nil {
		conds = append(conds, fmt.Sprintf("status = $%d", i))
		args = append(args, *f.Status)
		i++
	}
	if f.Item != nil {
		conds = append(conds, fmt.Sprintf("item ILIKE $%d", i))
		args = append(args, "%"+*f.Item+"%")
		i++
	}

	query := `SELECT id, user_id, item, quantity, status, created_at, updated_at
	          FROM orders_read WHERE ` + strings.Join(conds, " AND ")

	limit := f.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	offset := f.Offset
	if offset < 0 {
		offset = 0
	}
	query += fmt.Sprintf(" ORDER BY id DESC LIMIT $%d OFFSET $%d", i, i+1)
	args = append(args, limit, offset)

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var orders []domain.Order
	for rows.Next() {
		var o domain.Order
		if err := rows.Scan(&o.ID, &o.UserID, &o.Item, &o.Quantity, &o.Status, &o.CreatedAt, &o.UpdatedAt); err != nil {
			return nil, err
		}
		orders = append(orders, o)
	}
	return orders, rows.Err()
}
