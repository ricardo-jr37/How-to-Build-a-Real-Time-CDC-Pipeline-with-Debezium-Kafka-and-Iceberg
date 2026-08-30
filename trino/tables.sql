-- ---------------------------------------------------------------------------
-- The raw change-log tables, created explicitly.
--
-- The sink can auto-create these, but only by inferring types from the values
-- it sees. With schemaless JSON on the wire an epoch timestamp is just a
-- number, so `created_at` would land as bigint and day partitioning would be
-- impossible. Declaring the tables here keeps the messages lean *and* the
-- table typed -- and the schema of your lakehouse stops being a side effect of
-- whatever the first message happened to look like.
--
-- Every table carries the same five __ columns the ExtractNewRecordState SMT
-- appends. Partitioned by day of created_at.
-- ---------------------------------------------------------------------------

-- raw       -- append-only change log, one row per change event
-- lakehouse -- current-state views derived from raw
CREATE SCHEMA IF NOT EXISTS iceberg.raw;
CREATE SCHEMA IF NOT EXISTS iceberg.lakehouse;

CREATE TABLE IF NOT EXISTS iceberg.raw.customers (
    id              bigint,
    email           varchar,
    full_name       varchar,
    country         varchar,
    tier            varchar,
    created_at      timestamp(6) with time zone,
    updated_at      timestamp(6) with time zone,
    __deleted       varchar,
    __op            varchar,
    __ts_ms         bigint,
    __source_lsn    bigint,
    __source_table  varchar
) WITH (
    partitioning = ARRAY['day(created_at)'],
    format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS iceberg.raw.orders (
    id              bigint,
    customer_id     bigint,
    status          varchar,
    total_amount    double,
    currency        varchar,
    created_at      timestamp(6) with time zone,
    updated_at      timestamp(6) with time zone,
    __deleted       varchar,
    __op            varchar,
    __ts_ms         bigint,
    __source_lsn    bigint,
    __source_table  varchar
) WITH (
    partitioning = ARRAY['day(created_at)'],
    format = 'PARQUET'
);

CREATE TABLE IF NOT EXISTS iceberg.raw.clients (
    id              bigint,
    email           varchar,
    full_name       varchar,
    country         varchar,
    tier            varchar,
    phone_number    varchar,
    is_active       boolean,
    created_at      timestamp(6) with time zone,
    updated_at      timestamp(6) with time zone,
    __deleted       varchar,
    __op            varchar,
    __ts_ms         bigint,
    __source_lsn    bigint,
    __source_table  varchar
) WITH (
    partitioning = ARRAY['day(created_at)'],
    format = 'PARQUET'
);
