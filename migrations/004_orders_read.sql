CREATE TABLE IF NOT EXISTS orders_read (
    id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    item TEXT NOT NULL,
    quantity INT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    -- Дополнительные поля, которых нет в orders:
    last_event_at TIMESTAMPTZ NOT NULL, -- когда последний раз обновлялась
    event_count INT NOT NULL DEFAULT 0,
    deleted BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_orders_read_user_id ON orders_read (user_id);

CREATE INDEX IF NOT EXISTS idx_orders_read_status ON orders_read (status);

CREATE INDEX IF NOT EXISTS idx_orders_read_deleted ON orders_read (deleted);

-- Get-Content .\migrations\004_orders_read.sql | docker exec -i postgres psql -U user -d appdb