package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"my-go-service/internal/domain"
)

type OrderRepo struct {
	db *sql.DB
}

func NewOrderRepo(db *sql.DB) *OrderRepo {
	return &OrderRepo{db: db}
}

// ---------- CREATE ----------

func (r *OrderRepo) Create(ctx context.Context, req domain.CreateOrderRequest) (*domain.Order, error) {
	const query = `
		INSERT INTO orders (user_id, item, quantity)
		VALUES ($1, $2, $3)
		RETURNING id, user_id, item, quantity, status, created_at, updated_at
	`
	var o domain.Order
	err := r.db.QueryRowContext(ctx, query, req.UserID, req.Item, req.Quantity).
		Scan(&o.ID, &o.UserID, &o.Item, &o.Quantity, &o.Status, &o.CreatedAt, &o.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &o, nil
}

func (r *OrderRepo) CreateTx(ctx context.Context, tx *sql.Tx, req domain.CreateOrderRequest) (*domain.Order, error) {
	const query = `
		INSERT INTO orders (user_id, item, quantity)
		VALUES ($1, $2, $3)
		RETURNING id, user_id, item, quantity, status, created_at, updated_at
	`
	var o domain.Order
	err := tx.QueryRowContext(ctx, query, req.UserID, req.Item, req.Quantity).
		Scan(&o.ID, &o.UserID, &o.Item, &o.Quantity, &o.Status, &o.CreatedAt, &o.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &o, nil
}

// ---------- READ ----------

func (r *OrderRepo) GetByID(ctx context.Context, id int64) (*domain.Order, error) {
	const query = `
		SELECT id, user_id, item, quantity, status, created_at, updated_at
		FROM orders WHERE id = $1
	`
	var o domain.Order
	err := r.db.QueryRowContext(ctx, query, id).
		Scan(&o.ID, &o.UserID, &o.Item, &o.Quantity, &o.Status, &o.CreatedAt, &o.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &o, nil
}

func (r *OrderRepo) List(ctx context.Context, f domain.OrderFilter) ([]domain.Order, error) {
	var (
		conds []string
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

	query := `SELECT id, user_id, item, quantity, status, created_at, updated_at FROM orders`
	if len(conds) > 0 {
		query += " WHERE " + strings.Join(conds, " AND ")
	}

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

// ---------- UPDATE ----------

var ErrNoFieldsToUpdate = errors.New("no fields to update")

func (r *OrderRepo) Update(ctx context.Context, id int64, req domain.UpdateOrderRequest) (*domain.Order, error) {
	var (
		sets []string
		args []any
		i    = 1
	)

	if req.Item != nil {
		sets = append(sets, fmt.Sprintf("item = $%d", i))
		args = append(args, *req.Item)
		i++
	}
	if req.Quantity != nil {
		sets = append(sets, fmt.Sprintf("quantity = $%d", i))
		args = append(args, *req.Quantity)
		i++
	}
	if req.Status != nil {
		sets = append(sets, fmt.Sprintf("status = $%d", i))
		args = append(args, *req.Status)
		i++
	}
	if len(sets) == 0 {
		return nil, ErrNoFieldsToUpdate
	}
	sets = append(sets, "updated_at = NOW()")

	query := fmt.Sprintf(`
		UPDATE orders
		SET %s
		WHERE id = $%d
		RETURNING id, user_id, item, quantity, status, created_at, updated_at
	`, strings.Join(sets, ", "), i)
	args = append(args, id)

	var o domain.Order
	err := r.db.QueryRowContext(ctx, query, args...).
		Scan(&o.ID, &o.UserID, &o.Item, &o.Quantity, &o.Status, &o.CreatedAt, &o.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &o, nil
}

// ---------- DELETE ----------

func (r *OrderRepo) Delete(ctx context.Context, id int64) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM orders WHERE id = $1`, id)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return sql.ErrNoRows
	}
	return nil
}
