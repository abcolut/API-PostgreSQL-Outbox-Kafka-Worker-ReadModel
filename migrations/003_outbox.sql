CREATE TABLE IF NOT EXISTS outbox (
    id BIGSERIAL PRIMARY KEY,
    aggregate_id BIGINT NOT NULL, -- order_id
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending', -- pending | sent | failed
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW (),
    sent_at TIMESTAMPTZ
);

-- Индекс для relay: ищем только pending, сортируем по id
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox (id)
WHERE
    status = 'pending';

-- Get-Content .\migrations\003_outbox.sql | docker exec -i postgres psql -U user -d appdb