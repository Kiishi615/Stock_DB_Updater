"""
Convert every .jsonl file in data/ into a SQLite .db file.

Each DB has a single `prices` table with columns:
    date   TEXT  (YYYY-MM-DD, primary key)
    volume TEXT
    price  TEXT  (renamed from 'Adj')
"""

import json
import sqlite3
import os
import sys
from pathlib import Path


DATA_DIR = Path(__file__).parent / "data"
DB_DIR = Path(__file__).parent / "db"


def create_db(db_path: Path, records: list[dict]) -> int:
    """Create a SQLite DB with a `prices` table and insert records."""
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            date   TEXT PRIMARY KEY,
            volume TEXT,
            price  TEXT
        )
    """)
    cur.executemany(
        "INSERT OR IGNORE INTO prices (date, volume, price) VALUES (?, ?, ?)",
        [(r["date"], r["volume"], r["price"]) for r in records],
    )
    conn.commit()
    inserted = cur.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
    conn.close()
    return inserted


def parse_jsonl(jsonl_path: Path) -> list[dict]:
    """Read a .jsonl file and return normalised records."""
    records = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            records.append({
                "date":   obj.get("Date", ""),
                "volume": obj.get("Volume", "0"),
                "price":  obj.get("Adj", ""),
            })
    return records


def main():
    DB_DIR.mkdir(exist_ok=True)

    jsonl_files = sorted(DATA_DIR.glob("*.jsonl"))
    if not jsonl_files:
        print("No .jsonl files found in", DATA_DIR)
        sys.exit(1)

    print(f"Found {len(jsonl_files)} .jsonl files — converting…\n")

    total_records = 0
    for jf in jsonl_files:
        db_name = jf.stem + ".db"
        db_path = DB_DIR / db_name

        records = parse_jsonl(jf)
        inserted = create_db(db_path, records)
        total_records += inserted

        print(f"  ✓ {jf.name:<60} → {db_name}  ({inserted} rows)")

    print(f"\nDone! {len(jsonl_files)} databases, {total_records} total rows → {DB_DIR}")


if __name__ == "__main__":
    main()
