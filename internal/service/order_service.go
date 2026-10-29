package service

import (
	"context"
	"database/sql"
	"errors"
	"log"

	"my-go-service/internal/cache"
	"my-go-service/internal/domain"
	"my-go-service/internal/events"
	"my-go-service/internal/repository/postgres"
)

type OrderService struct {
	repo       *postgres.OrderRepo
	readRepo   *postgres.OrdersReadRepo
	cache      *cache.OrderCache
	outboxRepo *postgres.OutboxRepo
	db         *sql.DB
}

func NewOrderService(
	repo *postgres.OrderRepo,
	readRepo *postgres.OrdersReadRepo,
	c *cache.OrderCache,
	outbox *postgres.OutboxRepo,
	db *sql.DB,
) *OrderService {
	return &OrderService{
		repo: repo, readRepo: readRepo, cache: c, outboxRepo: outbox, db: db,
	}
}

// ---------- CREATE ----------

func (s *OrderService) Create(ctx context.Context, req domain.CreateOrderRequest) (*domain.Order, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	o, err := s.repo.CreateTx(ctx, tx, req)
	if err != nil {
		return nil, err
	}

	event := events.OrderEvent{
		Type:    "OrderCreated",
		OrderID: o.ID,
		Payload: o,
	}
	if err := s.outboxRepo.InsertTx(ctx, tx, o.ID, event.Type, event); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return o, nil
}

// ---------- READ (read-model) ----------

func (s *OrderService) GetByID(ctx context.Context, id int64) (*domain.Order, error) {
	// 1. Кэш
	o, err := s.cache.Get(ctx, id)
	if err == nil {
		log.Printf("cache HIT  order:%d", id)
		return o, nil
	}
	if !errors.Is(err, cache.ErrCacheMiss) {
		log.Printf("cache error: %v", err)
	}

	// 2. Read-модель
	o, err = s.readRepo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	// 3. Кэшируем
	if err := s.cache.Set(ctx, o); err != nil {
		log.Printf("cache set error: %v", err)
	}
	return o, nil
}

func (s *OrderService) List(ctx context.Context, f domain.OrderFilter) ([]domain.Order, error) {
	return s.readRepo.List(ctx, f)
}

// ---------- UPDATE ----------

func (s *OrderService) Update(ctx context.Context, id int64, req domain.UpdateOrderRequest) (*domain.Order, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	o, err := s.repo.Update(ctx, id, req) // упрощение: не UpdateTx
	if err != nil {
		return nil, err
	}

	event := events.OrderEvent{
		Type:    "OrderUpdated",
		OrderID: o.ID,
		Payload: o,
	}
	if err := s.outboxRepo.InsertTx(ctx, tx, o.ID, event.Type, event); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	s.invalidate(ctx, id)
	return o, nil
}

// ---------- DELETE ----------

func (s *OrderService) Delete(ctx context.Context, id int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := s.repo.Delete(ctx, id); err != nil {
		return err
	}

	event := events.OrderEvent{
		Type:    "OrderDeleted",
		OrderID: id,
	}
	if err := s.outboxRepo.InsertTx(ctx, tx, id, event.Type, event); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	s.invalidate(ctx, id)
	return nil
}

func (s *OrderService) invalidate(ctx context.Context, id int64) {
	if err := s.cache.Delete(ctx, id); err != nil {
		log.Printf("cache delete error: id=%d err=%v", id, err)
	} else {
		log.Printf("cache INVALIDATE order:%d", id)
	}
}
