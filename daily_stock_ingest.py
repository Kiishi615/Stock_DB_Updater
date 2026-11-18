"""
daily_stock_ingest.py — Fetch today's closing prices for all Nigerian stocks
from TradingView Screener and upsert them into the Postgres stock_prices table.

Usage:
    python daily_stock_ingest.py
    python daily_stock_ingest.py --dry-run

Prerequisites:
    pip install tradingview-screener psycopg2-binary python-dotenv

Set in .env:
    DATABASE_URL=postgresql://user:pass@host:5432/dbname
"""

import argparse
import json
import math
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from requests.exceptions import RequestException
from tradingview_screener import Query

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
load_dotenv(SCRIPT_DIR / ".env")

MAPPING_FILE = SCRIPT_DIR / "stock_mapping.json"
DATABASE_URL = os.environ.get("DATABASE_URL")
TELEGRAM_API_KEY = (os.environ.get("TELEGRAM_API_KEY") or "").strip() or None
TELEGRAM_CHAT_ID = (os.environ.get("TELEGRAM_CHAT_ID") or "").strip() or None

# West Africa Time = UTC+1
WAT = timezone(timedelta(hours=1))


# ---------------------------------------------------------------------------
# Load mapping
# ---------------------------------------------------------------------------
def load_mapping() -> dict[str, str]:
    """Load stock_mapping.json → {db_stock_name: tv_ticker}"""
    if not MAPPING_FILE.exists():
        print(f"ERROR: {MAPPING_FILE} not found.")
        print("Run the mapping generator first.")
        sys.exit(1)

    with open(MAPPING_FILE, encoding="utf-8") as f:
        mapping = json.load(f)

    # Filter out any empty values (removed bad matches leave blank lines)
    mapping = {k: v for k, v in mapping.items() if v}
    print(f"  Loaded {len(mapping)} stock mappings.")
    return mapping


# ---------------------------------------------------------------------------
# Fetch from TradingView
# ---------------------------------------------------------------------------
# Index tickers are used for reporting; all instruments are queried directly.
INDEX_TICKERS = [
    "NSENG:ASI",
    "NSENG:NGX30",
    "NSENG:NGXBNK",
    "NSENG:NGXINS",
    "NSENG:NGXOILGAS",
    "NSENG:NGXPENSION",
    "NSENG:NGXPREMIUM",
]


def fetch_all_nigerian_stocks(mapping: dict[str, str]) -> dict[str, dict]:
    """
    Fetch every mapped instrument directly, including funds and indices.
    Country scans omit some otherwise available symbols. Explicit tickers use
    the global scanner; do not add set_markets("nigeria") to these queries.
    Returns {ticker: {"close": float, "volume": int}, ...}
    """
    result = {}

    tickers = list(dict.fromkeys(mapping.values()))
    for offset in range(0, len(tickers), 100):
        batch = tickers[offset:offset + 100]
        query = (Query().select("name", "close", "volume")
                 .set_tickers(*batch).limit(len(batch)))
        for attempt in range(3):
            try:
                total, df = query.get_scanner_data(timeout=30)
                break
            except RequestException:
                if attempt == 2:
                    raise
                time.sleep(2 ** attempt)
        if total > len(df):
            raise RuntimeError(f"TradingView response truncated: {len(df)} of {total}")
        for _, row in df.iterrows():
            if row["ticker"] in batch:
                result[row["ticker"]] = {
                    "close": row.get("close"),
                    "volume": row.get("volume"),
                }

    idx_count = sum(ticker in INDEX_TICKERS for ticker in result)
    print(f"  Fetched {len(result) - idx_count} stocks/funds from TradingView.")
    print(f"  Fetched {idx_count} indices from TradingView.")

    return result


def missing_reason(data: dict | None) -> str | None:
    """Use the same validation for ingestion, console output and notifications."""
    if data is None:
        return "not returned by direct ticker query (check rename/delisting/coverage)"
    try:
        if data.get("close") is None or not math.isfinite(float(data["close"])):
            return "no finite close price"
        if float(data["close"]) <= 0:
            return "non-positive close price"
        volume = data.get("volume")
        # Missing volume is valid for indices and inactive trading sessions.
        if volume is not None and not math.isnan(float(volume)):
            if not math.isfinite(float(volume)) or float(volume) < 0:
                return "invalid volume"
    except (TypeError, ValueError, OverflowError):
        return "invalid numeric data"
    return None


# ---------------------------------------------------------------------------
# Build upsert rows
# ---------------------------------------------------------------------------
def build_rows(
    mapping: dict[str, str],
    tv_data: dict[str, dict],
    trade_date: str,
) -> list[tuple]:
    """
    Match DB stock names to TradingView data via the mapping.
    Returns list of (stock_name, date, volume, price) tuples.
    """
    rows = []
    matched = 0
    missing = []

    for name, ticker in mapping.items():
        reason = missing_reason(tv_data.get(ticker))
        if reason is None:
            data = tv_data[ticker]
            close = data.get("close")
            volume = data.get("volume")

            vol = int(float(volume)) if volume is not None and not math.isnan(float(volume)) else None
            prc = float(close)

            rows.append((name, trade_date, vol, prc))
            matched += 1
        else:
            missing.append((name, ticker, reason))

    print(f"  Matched: {matched} instruments")
    if missing:
        print(f"  Missing: {len(missing)} instruments (missing/invalid TradingView data)")
        for name, ticker, reason in missing[:10]:
            print(f"    - {name} ({ticker}): {reason}")
        if len(missing) > 10:
            print(f"    ... and {len(missing) - 10} more")

    return rows


# ---------------------------------------------------------------------------
# Upsert to Postgres
# ---------------------------------------------------------------------------
UPSERT_SQL = (
    "INSERT INTO stock_prices (stock_name, date, volume, price) "
    "VALUES %s "
    "ON CONFLICT (stock_name, date) DO UPDATE "
    "SET volume = EXCLUDED.volume, price = EXCLUDED.price"
)


def upsert_to_postgres(rows: list[tuple]) -> int:
    """Batch upsert rows into Postgres. Returns count of upserted rows."""
    if not DATABASE_URL:
        print("ERROR: DATABASE_URL not found in .env")
        sys.exit(1)

    pg = psycopg2.connect(DATABASE_URL)
    pg.autocommit = False
    cur = pg.cursor()

    try:
        execute_values(cur, UPSERT_SQL, rows, page_size=500)
        pg.commit()
        count = len(rows)
    except Exception as e:
        pg.rollback()
        print(f"  ERROR during upsert: {e}")
        count = 0
    finally:
        cur.close()
        pg.close()

    return count


# ---------------------------------------------------------------------------
# Telegram Report (sent AFTER upsert)
# ---------------------------------------------------------------------------
def send_telegram_report(result: dict):
    """Send a summary of the stock ingestion to Telegram."""
    if not TELEGRAM_API_KEY or not TELEGRAM_CHAT_ID:
        print("  Telegram not configured -- skipping notification.")
        return

    status = "OK" if not result.get("errors") else "ISSUES"
    lines = [
        f"[{status}] *Stock Price Ingestion*",
        f"{result['date']} ({result['day']})",
        "",
        "*Results:*",
        f"  Stocks upserted: *{result['stocks_upserted']}*",
        f"  Indices upserted: *{result['indices_upserted']}*",
        f"  Total rows: *{result['total_upserted']}*",
    ]

    if result.get("missing"):
        lines.append(f"  Missing: *{len(result['missing'])}*")
        for name in result["missing"][:5]:
            lines.append(f"    - {name}")

    lines.append("")
    lines.append(f"Elapsed: *{result['elapsed']:.1f}s*")

    if result.get("errors"):
        lines.append("")
        lines.append("*Errors:*")
        for err in result["errors"][:3]:
            lines.append(f"  - {err[:100]}")

    message = "\n".join(lines)

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_API_KEY}/sendMessage"
        payload = json.dumps({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
        }).encode("utf-8")
        req = urllib.request.Request(
            url, data=payload, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                print("  Telegram notification sent.")
            else:
                print(f"  Telegram responded with status {resp.status}")
    except Exception as e:
        print(f"  Failed to send Telegram notification: {e}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and validate only; no database writes or notifications")
    args = parser.parse_args(argv)
    start = time.time()
    now = datetime.now(WAT)
    today = now.strftime("%Y-%m-%d")

    # Skip weekends -- Nigerian Stock Exchange is closed Sat/Sun
    if now.weekday() >= 5 and not args.dry_run:
        print(f"Weekend ({now.strftime('%A')}) -- markets closed. Skipping.")
        sys.exit(0)

    print("=" * 60)
    print("  DAILY STOCK PRICE INGESTION")
    print("=" * 60)
    print(f"  Date:   {today} (WAT)")
    print()

    # 1. Load mapping
    print("[1/5] Loading stock mapping...")
    mapping = load_mapping()

    # 2. Fetch from TradingView
    print("\n[2/5] Fetching from TradingView Screener...")
    tv_data = fetch_all_nigerian_stocks(mapping)
    if not tv_data:
        print("  No data received. Exiting.")
        sys.exit(1)

    # 3. Build rows
    print("\n[3/5] Matching stocks...")
    rows = build_rows(mapping, tv_data, today)
    if not rows:
        print("  No rows to upsert. Exiting.")
        sys.exit(1)

    # Count stocks vs indices
    index_names = {name for name, ticker in mapping.items() if ticker in INDEX_TICKERS}
    indices_upserted = sum(1 for r in rows if r[0] in index_names)
    stocks_upserted = len(rows) - indices_upserted
    missing = [f"{name} ({ticker}): {reason}" for name, ticker in mapping.items()
               if (reason := missing_reason(tv_data.get(ticker))) is not None]
    errors = [f"{len(missing)} mapped instruments have missing/invalid data"] if missing else []

    if args.dry_run:
        print(f"\nDRY RUN: {stocks_upserted} stocks/funds + {indices_upserted} indices "
              f"= {len(rows)}/{len(mapping)} valid rows. No writes or notifications.")
        return 1 if missing else 0

    # 4. Upsert
    print(f"\n[4/5] Upserting {len(rows)} rows to Postgres...")
    count = upsert_to_postgres(rows)
    if count != len(rows):
        errors.append(f"Database upsert failed: {count}/{len(rows)} rows written")
        stocks_upserted = indices_upserted = 0

    elapsed = time.time() - start
    print()
    print("=" * 60)
    print(f"  INGESTION COMPLETE")
    print(f"  Date:     {today}")
    print(f"  Stocks:   {stocks_upserted}")
    print(f"  Indices:  {indices_upserted}")
    print(f"  Total:    {count} rows upserted")
    print(f"  Elapsed:  {elapsed:.1f}s")
    print("=" * 60)

    # 5. Telegram report (AFTER upsert)
    print("\n[5/5] Sending Telegram report...")
    send_telegram_report({
        "date": today,
        "day": now.strftime("%A"),
        "stocks_upserted": stocks_upserted,
        "indices_upserted": indices_upserted,
        "total_upserted": count,
        "missing": missing,
        "elapsed": elapsed,
        "errors": errors,
    })
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
