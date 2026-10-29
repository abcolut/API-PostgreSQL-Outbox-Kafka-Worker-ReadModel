package domain

import "time"

type Order struct {
	ID        int64     `json:"id"`
	UserID    int64     `json:"user_id"`
	Item      string    `json:"item"`
	Quantity  int       `json:"quantity"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type CreateOrderRequest struct {
	UserID   int64  `json:"user_id"`
	Item     string `json:"item"`
	Quantity int    `json:"quantity"`
}

type UpdateOrderRequest struct {
	Item     *string `json:"item,omitempty"`
	Quantity *int    `json:"quantity,omitempty"`
	Status   *string `json:"status,omitempty"`
}

type OrderFilter struct {
	UserID *int64  `json:"user_id,omitempty"`
	Status *string `json:"status,omitempty"`
	Item   *string `json:"item,omitempty"`   // поиск по подстроке
	Limit  int     `json:"limit,omitempty"`  // 1..100, default 20
	Offset int     `json:"offset,omitempty"` // >= 0, default 0
}
