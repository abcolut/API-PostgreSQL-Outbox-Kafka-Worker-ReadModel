CREATE TABLE IF NOT EXISTS order_events_log (
    id BIGSERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    order_id BIGINT NOT NULL,
    payload JSONB NOT NULL,
    kafka_topic TEXT NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW (),
    -- Ключ идемпотентности: одно и то же событие из Kafka
    -- (топик + партиция + offset) не может быть записано дважды.
    CONSTRAINT uq_kafka_position UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_order_events_log_order_id ON order_events_log (order_id, received_at DESC);

CREATE INDEX IF NOT EXISTS idx_order_events_log_type ON order_events_log (event_type, received_at DESC);

-- Get-Content .\migrations\002_order_events_log.sql | docker exec -i postgres psql -U user -d appdb
-- docker exec -it postgres psql -U user -d appdb -c "\d order_events_log"