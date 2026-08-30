# Real-Time CDC Pipeline: Postgres → Debezium → Kafka → Iceberg

A local, runnable stack for the article *How to Build a Real-Time CDC Pipeline
with Debezium, Kafka, and Iceberg*. Every row you change in Postgres shows up in
an Iceberg table roughly ten seconds later, queryable in SQL, with no batch job
anywhere in the path.

```
┌──────────┐   WAL    ┌──────────┐   JSON    ┌───────┐   Parquet   ┌──────────┐
│ Postgres │ ───────► │ Debezium │ ────────► │ Kafka │ ──────────► │  Iceberg │
│  (shop)  │  logical │  source  │           │       │   Iceberg   │ on MinIO │
└──────────┘   repl.  │ connector│           └───────┘  sink conn. └────┬─────┘
                      └──────────┘                                      │
                          Kafka Connect worker (one image, both halves)  │
                                                                   ┌────▼─────┐
                                                                   │  Trino   │
                                                                   └──────────┘
```

Seven containers; an eighth (Schema Registry) only if you switch to Avro. Kafka UI is there so you can see what is happening; nothing
depends on it.

## Quick start

Requires Docker with Compose v2, and about 6 GB of RAM free.

```bash
make up        # build, start, register connectors, create views  (~5 min first run)
make load      # push 2 minutes of inserts/updates/deletes at Postgres
make query     # see the change log and the current state in Iceberg
```

`make up` is idempotent — run it again any time.

| Service | URL | Notes |
|---|---|---|
| Kafka UI | http://localhost:28090 | topics, schemas, connectors, lag |
| Kafka Connect | http://localhost:28083 | REST API |
| Trino | http://localhost:28080 | query engine |
| Iceberg REST catalog | http://localhost:28181 | |
| Schema Registry | http://localhost:28081 | Avro mode only, see below |
| MinIO console | http://localhost:29001 | `admin` / `password` |
| Postgres | `localhost:25432` | `postgres` / `postgres`, db `shop` |
| Kafka bootstrap | `localhost:39092` | |

Every one of those ports is set in [.env](.env) and nowhere else. They are
deliberately off the defaults so the stack runs beside an existing Postgres or
Kafka; change them there if they still collide.

## What the pipeline does

**Postgres** (`shop`) has three tables — `customers`, `orders` and `clients` —
and runs with `wal_level=logical`. [postgres/init/01-cdc-setup.sql](postgres/init/01-cdc-setup.sql)
creates the replication role, sets `REPLICA IDENTITY FULL` so updates and
deletes carry a full before-image, and creates the publication explicitly rather
than letting the connector do it — the set of streamed tables belongs in source
control.

**Debezium** opens a logical replication slot and turns each committed row
change into a message on `shop.public.<table>`. Debezium's native envelope
nests the row inside `before` / `after` / `source` / `op`; the
`ExtractNewRecordState` SMT flattens that down to the row itself plus a handful
of `__` metadata columns, so a message reads like a record rather than a
document:

```json
{
  "id": 1, "customer_id": 1, "status": "shipped",
  "total_amount": 129.9, "currency": "USD",
  "created_at": 1787239816778, "updated_at": 1787239816778,
  "__op": "r", "__deleted": "false", "__ts_ms": 1787239850705,
  "__source_lsn": 26486440, "__source_table": "orders"
}
```

| column | meaning |
|---|---|
| `__op` | `r` snapshot read · `c` insert · `u` update · `d` delete |
| `__deleted` | `"true"` on a delete event, `"false"` otherwise |
| `__ts_ms` | when the change happened at the source, epoch millis |
| `__source_lsn` | Postgres WAL position — the real commit order |
| `__source_table` | which table it came from; the sink routes on this |

A delete would normally arrive with `after: null` and nothing to flatten.
`delete.tombstone.handling.mode=rewrite` turns it into the before-image with
`__deleted = true` instead, so a delete is a row like any other — exactly what an
append-only sink needs.

Read a topic with:

```bash
make consume                              # one event from shop.public.orders
make consume TOPIC=shop.public.customers N=3
```

**The Iceberg sink** consumes those topics and commits Parquet files to Iceberg
every 10 seconds. A commit is coordinated across tasks through a control topic,
so a snapshot never contains half a batch.

**Trino** reads the same REST catalog and the same bucket, so a query sees
exactly the last committed snapshot.

### JSON or Avro

The wire format is one line in [.env](.env):

```bash
CDC_CONVERTER=org.apache.kafka.connect.json.JsonConverter    # default
CDC_JSON_SCHEMAS_ENABLED=false
```

Schemaless JSON means the message is exactly the row and nothing else — no
`schema` block, no Schema Registry, readable with any console consumer.

The usual objection to schemaless JSON is that the sink then has to guess types:
an epoch timestamp is just a number, so `created_at` lands as `bigint` and day
partitioning becomes impossible. That objection only holds if you let the sink
create the tables. Here the tables come from explicit DDL in
[trino/tables.sql](trino/tables.sql), so `created_at` is declared
`timestamp(6) with time zone` and the sink converts the incoming number into it.
Lean messages and a properly typed table are not a trade-off — see
[Choices worth knowing about](#choices-worth-knowing-about).

Set `CDC_JSON_SCHEMAS_ENABLED=true` to put the schema back inline, or switch the
converter to `io.confluent.connect.avro.AvroConverter` for the compact binary
format you would run at volume — `make up` starts Schema Registry when it sees
Avro, and leaves it out otherwise. The Iceberg tables come out identical either
way.

## The two layers, and why## The two layers, and why## The two layers, and why

The Apache Iceberg Kafka Connect sink is **append-only**. It does not do
upserts, and it writes no equality deletes — every insert, update and delete
Debezium emits becomes one more row. So the lakehouse here has two layers:

**`raw.customers`, `raw.orders`, `raw.clients`** — the change log. One row per
change event, partitioned by day of `created_at`, carrying the `__` columns
above. Declared in [trino/tables.sql](trino/tables.sql).

**`lakehouse.customers`, `lakehouse.orders`, `lakehouse.clients`** — current
state, defined in
[trino/views.sql](trino/views.sql) as the newest event per primary key with
`__op = 'd'` filtered out. Ordering is by `__source_lsn`: the WAL position is the
order Postgres actually committed in, and unlike a millisecond timestamp it never
ties.

This split is not a workaround. Appending is why ingest stays cheap and
replayable; deriving current state at read time is what keeps a delete honest.
When the change log outgrows a view, the same query becomes a periodic `MERGE`
into a materialised table — the logic does not change, only where it runs.

The views name their columns explicitly, so after a schema change run
`make init` again to pick up the new column.

## Seeing it work

```bash
make psql
```
```sql
UPDATE orders SET status = 'refunded' WHERE id = 1;
DELETE FROM orders WHERE id = 2;
```

Within ~10 seconds:

```bash
make q SQL="SELECT id, status, __op, __source_lsn FROM iceberg.raw.orders WHERE id IN (1,2) ORDER BY __source_lsn"
make q SQL="SELECT id, status FROM iceberg.lakehouse.orders WHERE id IN (1,2)"
```

The raw table keeps all three events. The view shows the updated row 1, and
row 2 is gone.

### Soft delete vs hard delete

`clients` carries an `is_active` flag, which makes the difference easy to see:

```sql
UPDATE clients SET is_active = false WHERE id = 3;   -- soft delete
DELETE FROM clients WHERE id = 4;                    -- hard delete
```

```bash
make q SQL="SELECT id, full_name, is_active, __op, __deleted FROM iceberg.raw.clients WHERE id IN (3,4) ORDER BY __source_lsn"
make q SQL="SELECT id, full_name, is_active FROM iceberg.lakehouse.clients ORDER BY id"
```

Both are `__op = 'u'` and `'d'` in the change log. In the current-state view,
client 3 is still there with `is_active = false` — a business fact you can still
query — while client 4 is gone entirely. Only the source system decides which
kind of delete a row gets; the pipeline carries both faithfully.

### Schema evolution

```bash
make psql
```
```sql
ALTER TABLE orders ADD COLUMN discount_code TEXT;
INSERT INTO orders (customer_id, status, total_amount, discount_code)
VALUES (1, 'pending', 50.00, 'BLACKFRIDAY');
```

`iceberg.tables.evolve-schema-enabled` makes the sink add the column to the
Iceberg table on the next batch. No downtime, no rewrite:

```bash
make q SQL="DESCRIBE iceberg.raw.orders"
make init     # refresh the views so they expose the new column
```

### Checking the plumbing

```bash
make status
```

Prints the replication slot (including how much WAL Postgres is retaining for
it — the number that pages you at 3am when a connector dies), the CDC topics,
both connector states, the sink's **commit health**, and consumer lag.

Commit health is there because of a failure mode worth knowing about.

### When the sink goes quiet

The Iceberg sink can stop writing while looking perfectly healthy. You will see
the snapshot land, and then nothing — no new rows, no failed task.

It happens when a sink task is stopped and its commit coordinator thread
outlives it, still holding a REST catalog client that was closed with the task.
The zombie keeps answering on the control topic, and every commit from then on
dies with `IllegalStateException: Connection pool shut down`. Meanwhile:

- the connector reports `RUNNING`, and so do its tasks
- the consumer group shows **zero lag** — records are read, just never written
- the only symptom is `committed to 0 table(s)` repeating in the worker log

`make status` checks for exactly this and tells you if it is happening.

The fix is to restart the **worker**, not the connector — the thread lives in
the worker process, so `POST /connectors/iceberg-sink/restart` does nothing:

```bash
make restart-connect
```

Nothing is lost. Offsets are tracked in the Iceberg snapshots, so the sink
replays everything it had read and the tables catch up.

The reliable way to trigger it is re-submitting a connector config, which
restarts the tasks. `make up` used to do that on every run; now
[scripts/register-connectors.sh](scripts/register-connectors.sh) compares the
desired config against what is running and leaves it alone when they match, so
running `make up` repeatedly is genuinely safe. The sink also runs with
`tasks.max=1`, which keeps a second task from being stopped underneath the
coordinator in the first place.

## Layout

```
docker-compose.yml            the whole stack
.env                          every host port and credential
Makefile                      make help lists everything
connect/
  Dockerfile                  Connect worker: Debezium + Iceberg sink
  pom.xml                     assembles the Iceberg sink plugin
connectors/
  01-postgres-source.json     Debezium connector config
  02-iceberg-sink.json        Iceberg sink connector config
postgres/init/01-cdc-setup.sql  role, tables, publication, seed data
trino/
  catalog/iceberg.properties  Trino → REST catalog → MinIO
  tables.sql                  raw change-log tables (explicit DDL)
  views.sql                   current-state views
scripts/
  register-connectors.sh      idempotent connector registration
  init-lakehouse.sh           namespaces, raw tables and views
  run-sql.sh                  runs a .sql file through Trino
  wait-for-stack.sh           blocks until every service is healthy
  inspect-cdc.sh              slot, topics, connector state, commit health, lag
  query-iceberg.sh            summary queries, or run your own SQL
  generate-load.py            continuous write load
```

## Choices worth knowing about

**The Iceberg plugin is built, not downloaded.** Apache publishes
`iceberg-kafka-connect` to Maven Central but ships no prebuilt runtime bundle,
so [connect/pom.xml](connect/pom.xml) resolves the connector plus what it needs
at runtime — Parquet writers, `S3FileIO`, the shaded AWS SDK, and a trimmed
`hadoop-common` that Parquet's writer API still depends on.

**The warehouse bucket is a directory.** MinIO exposes any top-level directory
in its data dir as a bucket, so the container creates `/data/warehouse` on
startup and the init container that used to do it is gone.

**Timestamps are `TIMESTAMP`, not `TIMESTAMPTZ`.** Debezium encodes
`timestamptz` as an ISO-8601 string; a plain `TIMESTAMP` arrives as a number the
sink can convert into the declared Iceberg timestamp column.

**`decimal.handling.mode=double`.** Debezium's default encodes numerics as
base64 bytes plus a scale. `double` keeps the demo readable; for money in
production use `precise` and let the sink write a real Iceberg `decimal`.

**`tombstones.on.delete=false`.** A tombstone is a null-valued record, and an
append-only sink has nothing useful to do with one. The delete already arrives
as its own event with `__op = 'd'`.

**The REST catalog is SQLite in WAL mode** with a busy timeout, on a volume so
it survives a restart. Without WAL, concurrent table commits from the sink hit
`SQLITE_BUSY`. Any real deployment points this at Postgres, Glue, Nessie or
Polaris instead.

**The tables are declared, not inferred.** `iceberg.tables.auto-create-enabled`
is `false` and [trino/tables.sql](trino/tables.sql) owns the schema. That is what
lets the topics carry schemaless JSON without the lakehouse paying for it: the
sink is converting into a known column type instead of guessing from a value.
It also means the shape of your tables is reviewable in a diff rather than a
side effect of whichever message arrived first.

**Timestamps must be millis.** The sink multiplies a bare number by 1000 before
reading it as a timestamp, so `time.precision.mode=connect` (epoch millis) is
required. `microseconds` looks more precise and silently lands every row in the
year 58611.

**`tasks.max=1` on the sink.** Not a throughput decision — see
[When the sink goes quiet](#when-the-sink-goes-quiet). More tasks means more
task stops, and a stopped task is what strands the commit coordinator.

**The sink routes on `__source_table`.** One sink connector feeds three tables.
`iceberg.table.<name>.route-regex` matches against the value of
`iceberg.tables.route-field`, not against the topic name — leave the route field
unset and every record fans out to every table.

**Schema Registry is a Compose profile.** Switching the converter to JSON drops
it from the stack entirely — it is not a service you have to remember to stop.

**One Connect worker runs both connectors.** Fine for a laptop. In production
the source and sink belong in separate worker groups so a sink backlog cannot
slow WAL consumption — and a stalled Debezium connector means Postgres retains
WAL until the disk fills.

## Teardown

```bash
make down     # stop, keep the data
make clean    # stop and delete every volume
```
