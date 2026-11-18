# Stock_DB_Updater

Daily automated ingestion of Nigerian Stock Exchange (NGX) prices into PostgreSQL.

## What it does

- Fetches **latest prices + volume** for 129 Nigerian stocks/funds and 7 NGX indices from TradingView using direct ticker queries
- **Batch upserts** into a Postgres `stock_prices` table (Neon/Supabase)
- Sends a **Telegram report** after each run
- Runs **Mon–Fri at 5:00 PM WAT** via GitHub Actions

## Files

| File | Purpose |
|---|---|
| `daily_stock_ingest.py` | Main ingestion script |
| `stock_mapping.json` | Maps DB stock names → TradingView tickers |
| `retired_stocks.json` | Retired instruments, dates, reasons and sources |
| `INGESTION_FINDINGS.md` | Missing-symbol investigation and live verification |
| `test_daily_stock_ingest.py` | Offline regression tests |
| `migrate_to_postgres.py` | One-time migration of local SQLite DBs → Postgres |
| `jsonl_to_db.py` | Converts scraped JSONL → SQLite `.db` files |

## Setup

### GitHub Secrets Required

| Secret | Description |
|---|---|
| `DATABASE_URL` | Postgres connection string |
| `TELEGRAM_API_KEY` | Telegram bot token |
| `TELEGRAM_CHAT_ID` | Telegram chat ID |

### Run manually

```bash
pip install tradingview-screener==3.0.0 psycopg2-binary python-dotenv
python daily_stock_ingest.py
```

Validate live source coverage without database writes or Telegram messages:

```bash
python daily_stock_ingest.py --dry-run
```

Run offline regression tests:

```bash
python -m unittest -v
```

Missing or invalid mapped data now produces an `ISSUES` notification and exit
code 1, so GitHub Actions flags incomplete runs. Valid rows are still ingested.
Update mappings after verified ticker changes; retain existing database names
to keep price history continuous. Record confirmed delistings in
`retired_stocks.json` before removing them from the active mapping.

The screener supplies the latest available quote, which may be delayed or old
for suspended/illiquid securities. A successful coverage check verifies that
prices are returned, not that every instrument traded that day. Run after the
market closes for closing-price ingestion. This change does not backfill gaps
from previous runs.
