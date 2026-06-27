# Stock_DB_Updater

Daily automated ingestion of Nigerian Stock Exchange (NGX) prices into PostgreSQL.

## What it does

- Fetches **closing prices + volume** for ~130 Nigerian stocks and 7 NGX indices from TradingView
- **Batch upserts** into a Postgres `stock_prices` table (Neon/Supabase)
- Sends a **Telegram report** after each run
- Runs **Mon–Fri at 5:00 PM WAT** via GitHub Actions

## Files

| File | Purpose |
|---|---|
| `daily_stock_ingest.py` | Main ingestion script |
| `stock_mapping.json` | Maps DB stock names → TradingView tickers |
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
pip install tradingview-screener psycopg2-binary python-dotenv
python daily_stock_ingest.py
```
