-- ---------------------------------------------------------------------------
-- Current state, derived from the append-only change log.
--
-- The Iceberg Kafka Connect sink appends: every INSERT, UPDATE and DELETE
-- Debezium emits becomes one more row in raw.<table>, tagged by __op. Nothing
-- is ever overwritten, which is what makes the ingest side cheap and replayable.
--
-- "The current row for each key" is therefore a query, not a table: take the
-- newest event per primary key and drop it if that event was a delete.
--
-- Ordering is by __source_lsn, the Postgres WAL position. It is the actual
-- order the database committed things in, and unlike a millisecond timestamp
-- it never ties.
--
--   __op  r snapshot read · c insert · u update · d delete
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW iceberg.lakehouse.customers AS
SELECT id, email, full_name, country, tier, created_at, updated_at,
       CAST(from_unixtime(__ts_ms / 1e3, 'UTC') AS timestamp(6) with time zone) AS _last_change_at
FROM (
    SELECT *,
           row_number() OVER (
               PARTITION BY id ORDER BY __source_lsn DESC
           ) AS _rn
    FROM iceberg.raw.customers
) t
WHERE _rn = 1
  AND __op <> 'd';

CREATE OR REPLACE VIEW iceberg.lakehouse.orders AS
SELECT id, customer_id, status, total_amount, currency, created_at, updated_at,
       CAST(from_unixtime(__ts_ms / 1e3, 'UTC') AS timestamp(6) with time zone) AS _last_change_at
FROM (
    SELECT *,
           row_number() OVER (
               PARTITION BY id ORDER BY __source_lsn DESC
           ) AS _rn
    FROM iceberg.raw.orders
) t
WHERE _rn = 1
  AND __op <> 'd';

CREATE OR REPLACE VIEW iceberg.lakehouse.clients AS
SELECT id, email, full_name, country, tier, phone_number, is_active,
       created_at, updated_at,
       CAST(from_unixtime(__ts_ms / 1e3, 'UTC') AS timestamp(6) with time zone) AS _last_change_at
FROM (
    SELECT *,
           row_number() OVER (
               PARTITION BY id ORDER BY __source_lsn DESC
           ) AS _rn
    FROM iceberg.raw.clients
) t
WHERE _rn = 1
  AND __op <> 'd';
