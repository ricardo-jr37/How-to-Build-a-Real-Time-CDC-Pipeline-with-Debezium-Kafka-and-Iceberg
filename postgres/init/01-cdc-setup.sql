-- ---------------------------------------------------------------------------
-- Everything Debezium needs on the Postgres side.
--
-- Runs once, on first boot of an empty data directory. Three things matter:
--   1. a role with REPLICATION so it can open a logical replication slot
--   2. a publication naming the tables to stream
--   3. REPLICA IDENTITY FULL so UPDATE/DELETE carry a full "before" image
-- ---------------------------------------------------------------------------

-- 1. Replication role ------------------------------------------------------
CREATE ROLE debezium WITH REPLICATION LOGIN PASSWORD 'debezium';
GRANT CONNECT ON DATABASE shop TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;


-- 2. Business tables -------------------------------------------------------
-- Timestamps are TIMESTAMP (UTC) rather than TIMESTAMPTZ on purpose. Debezium
-- encodes timestamptz as an ISO-8601 *string* (io.debezium.time.ZonedTimestamp),
-- which the Iceberg sink can only land as a string column. A plain TIMESTAMP
-- with time.precision.mode=connect arrives as epoch micros and becomes a real
-- Iceberg timestamp you can filter and partition on.
CREATE TABLE customers (
    id          BIGSERIAL PRIMARY KEY,
    email       TEXT        NOT NULL UNIQUE,
    full_name   TEXT        NOT NULL,
    country     TEXT        NOT NULL,
    tier        TEXT        NOT NULL DEFAULT 'standard',
    created_at  TIMESTAMP   NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    updated_at  TIMESTAMP   NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE TABLE orders (
    id            BIGSERIAL PRIMARY KEY,
    customer_id   BIGINT      NOT NULL REFERENCES customers (id),
    status        TEXT        NOT NULL DEFAULT 'pending',
    total_amount  NUMERIC(12,2) NOT NULL,
    currency      TEXT        NOT NULL DEFAULT 'USD',
    created_at    TIMESTAMP   NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    updated_at    TIMESTAMP   NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE INDEX orders_customer_id_idx ON orders (customer_id);

-- A third table, deliberately a bit different from the other two: it carries a
-- boolean and free-text phone numbers, and its rows are seeded with explicit
-- ids. Useful for watching a soft delete (is_active = false) and a hard DELETE
-- travel down the same pipeline and land as different things in Iceberg.
CREATE TABLE clients (
    id            BIGSERIAL PRIMARY KEY,
    email         TEXT        NOT NULL UNIQUE,
    full_name     TEXT        NOT NULL,
    country       TEXT        NOT NULL,
    tier          TEXT        NOT NULL DEFAULT 'bronze',
    phone_number  TEXT,
    is_active     BOOLEAN     NOT NULL DEFAULT true,
    created_at    TIMESTAMP   NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    updated_at    TIMESTAMP   NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);

-- Keep updated_at honest so downstream freshness checks mean something.
CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := now() AT TIME ZONE 'utc';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER customers_touch BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER orders_touch BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
CREATE TRIGGER clients_touch BEFORE UPDATE ON clients
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();


-- 3. Make the WAL carry enough to reconstruct a change --------------------
-- DEFAULT would only log the primary key on UPDATE/DELETE. FULL logs the whole
-- old row, which is what gives Debezium a usable `before` image.
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE orders    REPLICA IDENTITY FULL;
ALTER TABLE clients   REPLICA IDENTITY FULL;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;


-- 4. Publication Debezium subscribes to ------------------------------------
-- Created explicitly rather than letting the connector do it, so the set of
-- streamed tables is visible in source control instead of hidden in a config.
CREATE PUBLICATION dbz_publication FOR TABLE customers, orders, clients;


-- 5. Seed data -------------------------------------------------------------
INSERT INTO customers (email, full_name, country, tier) VALUES
    ('ada@example.com',     'Ada Lovelace',    'GB', 'gold'),
    ('alan@example.com',    'Alan Turing',     'GB', 'gold'),
    ('grace@example.com',   'Grace Hopper',    'US', 'silver'),
    ('linus@example.com',   'Linus Torvalds',  'FI', 'standard'),
    ('margaret@example.com','Margaret Hamilton','US','gold');

INSERT INTO orders (customer_id, status, total_amount, currency) VALUES
    (1, 'shipped',   129.90, 'USD'),
    (1, 'pending',    45.00, 'USD'),
    (2, 'delivered', 899.99, 'USD'),
    (3, 'pending',    19.90, 'USD'),
    (4, 'cancelled', 240.00, 'EUR'),
    (5, 'shipped',    75.50, 'USD');

INSERT INTO clients (
    id,
    email,
    full_name,
    country,
    tier,
    phone_number,
    is_active
)
VALUES
(
    1,
    'ricardo@example.com',
    'Ricardo Junior',
    'Brazil',
    'gold',
    '+55 85 99999-1111',
    true
),
(
    2,
    'john@example.com',
    'John Smith',
    'United States',
    'silver',
    '+1 202 555 0101',
    true
),
(
    3,
    'anna@example.com',
    'Anna Müller',
    'Germany',
    'bronze',
    '+49 30 123456',
    true
),
(
    4,
    'maria@example.com',
    'Maria Silva',
    'Brazil',
    'silver',
    '+55 11 98888-2222',
    true
);

-- The rows above set id explicitly, which leaves the sequence at 1. Nudge it
-- past the seeded ids so later inserts do not collide.
SELECT setval(pg_get_serial_sequence('clients', 'id'), (SELECT max(id) FROM clients));
