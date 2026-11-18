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
| `stock_identities.json` | TradingView names, ISINs and types verified from live direct quotes |
| `ticker_recovery_report.json` | Generated audit of recoveries and unresolved instruments (ignored by Git) |
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

Validate live source coverage and simulate recovery without file/database writes or Telegram messages:

```bash
python daily_stock_ingest.py --dry-run
```

Run offline regression tests:

```bash
python -m unittest -v
```

Missing or invalid mapped data now produces an `ISSUES` notification and exit
code 1, so GitHub Actions flags incomplete runs. Valid rows are still ingested.
Failed tickers are retried together first. Remaining failures are searched in
TradingView's NSENG catalogue using their stored TradingView names, database
names, and ISINs. Symbol-page redirects supply additional candidates when a
company has completely renamed itself. A replacement must return a valid
direct quote with the **same stored ISIN and instrument type**. There must be
exactly one verified replacement, and it cannot belong to another mapping.
Similar names alone never authorize a change.

All recovery attempts finish before row construction and upsert. Verified
repairs atomically update `stock_mapping.json` and refresh the corresponding
TradingView name/ticker in `stock_identities.json`. Database names remain
unchanged, keeping price history continuous. These reference files must both
be committed when deploying; `git add -u` does not include a new identity file.
The initial identities were captured from live TradingView quotes for all 136
mapped instruments on 18 November 2026. All 129 stocks/funds have an ISIN.
The seven indices lack ISINs: original-ticker retries work for them, but
automatic ticker replacement is deliberately disabled without stable identity.

Unknown/ambiguous replacements, changed ISINs, delistings, or unavailable
sources stay unresolved and are excluded from ingestion. Valid instruments
continue. The console and `ticker_recovery_report.json` record the reasons;
the Telegram summary includes recovered tickers and missing instruments.
Recovery has a two-minute budget, checked between lookups, in addition to
bounded request timeouts/retries; unresolved items are retried on the next run.
Record confirmed delistings in
`retired_stocks.json` before removing them from the active mapping.

GitHub Actions serializes runs per branch and commits only verified mapping
and identity changes back to that branch, even when other stocks remain
unresolved. The job grants `contents: write` and uploads recovery reports as
artifacts for 30 days. Repository/branch rules must permit the Actions bot to
push those two files. A rejected push fails visibly; it is never force-pushed.
Offline tests can be run separately and are not added to the daily job.

The screener supplies the latest available quote, which may be delayed or old
for suspended/illiquid securities. A successful coverage check verifies that
prices are returned, not that every instrument traded that day. Run after the
market closes for closing-price ingestion. This change does not backfill gaps
from previous runs.
