"""
migrate_to_postgres.py — Consolidate 145 local SQLite stock databases
into a single PostgreSQL table on Supabase (or any Postgres host).

Usage:
    python migrate_to_postgres.py

Prerequisites:
    pip install psycopg2-binary python-dotenv

Set in .env:
    DATABASE_URL=postgresql://user:pass@host:5432/dbname
"""

import os
import sqlite3
import sys
import time
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
load_dotenv(SCRIPT_DIR / ".env")

DB_DIR = SCRIPT_DIR / "db"
DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    print("ERROR: DATABASE_URL not found in .env")
    print("Set it to your Supabase/Neon PostgreSQL connection string.")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_prices (
    stock_name  TEXT        NOT NULL,
    date        DATE        NOT NULL,
    volume      BIGINT,
    price       NUMERIC(12, 4),
    PRIMARY KEY (stock_name, date)
);
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_stock_prices_stock_date
ON stock_prices (stock_name, date DESC);
"""


# ---------------------------------------------------------------------------
# Migration
# ---------------------------------------------------------------------------
def migrate():
    print("=" * 60)
    print("  STOCK DATABASE MIGRATION → PostgreSQL")
    print("=" * 60)
    print(f"  Source:  {DB_DIR}")
    print(f"  Target:  {DATABASE_URL[:40]}...")
    print()

    db_files = sorted(DB_DIR.glob("*.db"))
    if not db_files:
        print("No .db files found in", DB_DIR)
        sys.exit(1)

    print(f"Found {len(db_files)} SQLite databases.\n")

    # Connect to PostgreSQL
    pg = psycopg2.connect(DATABASE_URL)
    pg.autocommit = False
    cur = pg.cursor()

    # Create table + index
    cur.execute(CREATE_TABLE_SQL)
    cur.execute(CREATE_INDEX_SQL)
    pg.commit()
    print("✓ Table 'stock_prices' ready.\n")

    total_rows = 0
    errors = []
    start = time.time()

    for i, db_file in enumerate(db_files, 1):
        stock_name = db_file.stem

        try:
            # Read local SQLite
            conn = sqlite3.connect(str(db_file))
            rows = conn.cursor().execute(
                "SELECT date, volume, price FROM prices ORDER BY date"
            ).fetchall()
            conn.close()

            if not rows:
                print(f"  [{i:3d}/{len(db_files)}] {stock_name}: 0 rows (skipped)")
                continue

            # Parse volume/price — they're stored as TEXT in SQLite
            cleaned = []
            for date_val, volume, price in rows:
                try:
                    vol = int(volume) if volume else None
                except (ValueError, TypeError):
                    vol = None
                try:
                    prc = float(price) if price else None
                except (ValueError, TypeError):
                    prc = None
                cleaned.append((stock_name, date_val, vol, prc))

            # Batch upsert into PostgreSQL (1000 rows per network call)
            execute_values(
                cur,
                "INSERT INTO stock_prices (stock_name, date, volume, price) "
                "VALUES %s "
                "ON CONFLICT (stock_name, date) DO UPDATE "
                "SET volume = EXCLUDED.volume, price = EXCLUDED.price",
                cleaned,
                page_size=1000,
            )

            pg.commit()
            total_rows += len(rows)
            print(f"  [{i:3d}/{len(db_files)}] {stock_name}: {len(rows)} rows ✓")

        except Exception as e:
            pg.rollback()
            errors.append((stock_name, str(e)))
            print(f"  [{i:3d}/{len(db_files)}] {stock_name}: ERROR — {e}")

    cur.close()
    pg.close()

    elapsed = time.time() - start
    print()
    print("=" * 60)
    print(f"  MIGRATION COMPLETE")
    print(f"  Stocks migrated : {len(db_files) - len(errors)}/{len(db_files)}")
    print(f"  Total rows      : {total_rows:,}")
    print(f"  Errors           : {len(errors)}")
    print(f"  Elapsed          : {elapsed:.1f}s")
    print("=" * 60)

    if errors:
        print("\nFailed stocks:")
        for name, err in errors:
            print(f"  ✗ {name}: {err}")


if __name__ == "__main__":
    migrate()
