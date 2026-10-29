package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"my-go-service/internal/domain"
)

var ErrCacheMiss = errors.New("cache miss")

type OrderCache struct {
	rdb *redis.Client
	ttl time.Duration
}

func NewOrderCache(rdb *redis.Client, ttl time.Duration) *OrderCache {
	return &OrderCache{rdb: rdb, ttl: ttl}
}

// Ключ вида "order:123"
func orderKey(id int64) string {
	return fmt.Sprintf("order:%d", id)
}

// Get возвращает ErrCacheMiss, если ключа нет или не удалось распарсить.
func (c *OrderCache) Get(ctx context.Context, id int64) (*domain.Order, error) {
	raw, err := c.rdb.Get(ctx, orderKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrCacheMiss
	}
	if err != nil {
		return nil, err
	}

	var o domain.Order
	if err := json.Unmarshal(raw, &o); err != nil {
		// Битые данные в кэше — не повод падать. Просто игнорируем.
		return nil, ErrCacheMiss
	}
	return &o, nil
}

// Set кладёт объект в кэш с TTL.
func (c *OrderCache) Set(ctx context.Context, o *domain.Order) error {
	raw, err := json.Marshal(o)
	if err != nil {
		return err
	}
	return c.rdb.Set(ctx, orderKey(o.ID), raw, c.ttl).Err()
}

// Delete инвалидирует ключ. Не ошибка, если ключа не было.
func (c *OrderCache) Delete(ctx context.Context, id int64) error {
	return c.rdb.Del(ctx, orderKey(id)).Err()
}
