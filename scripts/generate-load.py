#!/usr/bin/env python3
"""
Continuous write load against the source database, so the pipeline has
something to stream. Mixes inserts, updates and deletes -- the three CDC
operations that the Iceberg sink has to translate into appends and
equality deletes.

    python3 scripts/generate-load.py --rate 5 --duration 120

Requires psycopg2 (`pip install psycopg2-binary`), or run it through Docker:

    make load
"""
import argparse
import os
import random
import string
import sys
import time

try:
    import psycopg2
except ImportError:
    sys.exit("psycopg2 is required:  pip install psycopg2-binary")

COUNTRIES = ["US", "GB", "BR", "DE", "FI", "JP", "IN"]
TIERS = ["standard", "silver", "gold"]
STATUSES = ["pending", "shipped", "delivered", "cancelled"]
COUNTRY_NAMES = ["Brazil", "United States", "Germany", "Finland", "Japan", "India"]
CLIENT_TIERS = ["bronze", "silver", "gold"]


def rand_email() -> str:
    user = "".join(random.choices(string.ascii_lowercase, k=8))
    return f"{user}@example.com"


def insert_customer(cur) -> int:
    cur.execute(
        """INSERT INTO customers (email, full_name, country, tier)
           VALUES (%s, %s, %s, %s) RETURNING id""",
        (rand_email(), "".join(random.choices(string.ascii_uppercase, k=6)),
         random.choice(COUNTRIES), random.choice(TIERS)),
    )
    return cur.fetchone()[0]


def insert_order(cur) -> None:
    cur.execute("SELECT id FROM customers ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return
    cur.execute(
        """INSERT INTO orders (customer_id, status, total_amount, currency)
           VALUES (%s, %s, %s, %s)""",
        (row[0], random.choice(STATUSES),
         round(random.uniform(5, 2000), 2), random.choice(["USD", "EUR", "BRL"])),
    )


def update_order(cur) -> None:
    cur.execute(
        """UPDATE orders SET status = %s
           WHERE id = (SELECT id FROM orders ORDER BY random() LIMIT 1)""",
        (random.choice(STATUSES),),
    )


def update_customer(cur) -> None:
    cur.execute(
        """UPDATE customers SET tier = %s
           WHERE id = (SELECT id FROM customers ORDER BY random() LIMIT 1)""",
        (random.choice(TIERS),),
    )


def insert_client(cur) -> None:
    cur.execute(
        """INSERT INTO clients (email, full_name, country, tier, phone_number)
           VALUES (%s, %s, %s, %s, %s)""",
        (rand_email(), "".join(random.choices(string.ascii_uppercase, k=6)),
         random.choice(COUNTRY_NAMES), random.choice(CLIENT_TIERS),
         f"+55 85 9{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"),
    )


def deactivate_client(cur) -> None:
    """A soft delete: the row stays, is_active flips. Arrives as _cdc.op = 'U'."""
    cur.execute(
        """UPDATE clients SET is_active = NOT is_active
           WHERE id = (SELECT id FROM clients ORDER BY random() LIMIT 1)"""
    )


def update_client_tier(cur) -> None:
    cur.execute(
        """UPDATE clients SET tier = %s
           WHERE id = (SELECT id FROM clients ORDER BY random() LIMIT 1)""",
        (random.choice(CLIENT_TIERS),),
    )


def delete_order(cur) -> None:
    cur.execute(
        """DELETE FROM orders
           WHERE id = (SELECT id FROM orders ORDER BY random() LIMIT 1)"""
    )


# Weighted so the stream looks like a real OLTP workload: mostly writes and
# updates, the occasional delete.
ACTIONS = (
    [insert_order] * 5
    + [update_order] * 3
    + [insert_customer] * 2
    + [update_customer] * 2
    + [insert_client] * 2
    + [update_client_tier] * 2
    + [deactivate_client] * 1
    + [delete_order] * 1
)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--host", default=os.environ.get("PGHOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.environ.get("PGPORT", 15432)))
    p.add_argument("--dbname", default=os.environ.get("PGDATABASE", "shop"))
    p.add_argument("--user", default=os.environ.get("PGUSER", "postgres"))
    p.add_argument("--password", default=os.environ.get("PGPASSWORD", "postgres"))
    p.add_argument("--rate", type=float, default=5.0, help="statements per second")
    p.add_argument("--duration", type=int, default=0, help="seconds; 0 = run forever")
    args = p.parse_args()

    conn = psycopg2.connect(
        host=args.host, port=args.port, dbname=args.dbname,
        user=args.user, password=args.password,
    )
    conn.autocommit = True

    interval = 1.0 / args.rate if args.rate > 0 else 0
    deadline = time.time() + args.duration if args.duration else None
    n = 0

    print(f"writing ~{args.rate}/s to {args.host}:{args.port}/{args.dbname} "
          f"({'forever' if not deadline else str(args.duration) + 's'}) -- ctrl-c to stop")
    try:
        while deadline is None or time.time() < deadline:
            with conn.cursor() as cur:
                random.choice(ACTIONS)(cur)
            n += 1
            if n % 25 == 0:
                print(f"  {n} statements")
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        conn.close()
        print(f"done -- {n} statements issued")


if __name__ == "__main__":
    main()
