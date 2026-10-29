-- CREATE TABLE IF NOT EXISTS orders (
--     id BIGSERIAL PRIMARY KEY,
--     user_id BIGINT NOT NULL,
--     item TEXT NOT NULL,
--     quantity INT NOT NULL DEFAULT 1,
--     status TEXT NOT NULL DEFAULT 'created',
--     created_at TIMESTAMPTZ NOT NULL DEFAULT NOW ()
-- );
-- CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders (user_id);
CREATE TABLE IF NOT EXISTS orders (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    item TEXT NOT NULL,
    quantity INT NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'created',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW (),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW ()
);

CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders (user_id);

CREATE INDEX IF NOT EXISTS idx_orders_status ON orders (status);