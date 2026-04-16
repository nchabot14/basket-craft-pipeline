# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Python Environment

Use the virtual environment for all Python commands — never the system Python.

```bash
source .venv/bin/activate   # activate (optional — use prefixed commands below)
.venv/bin/python3 pipeline.py   # run the pipeline
.venv/bin/pytest tests/ -v      # run all tests
.venv/bin/pytest tests/test_transform.py -v          # unit tests only (no DB needed)
.venv/bin/pytest tests/test_pipeline.py -k "extract" # single test group
```

Install dependencies:
```bash
python3 -m pip install -r requirements.txt
```

> **Note:** `psycopg2-binary==2.9.9` is pinned (not 2.9.10) — 2.9.10 has no pre-built ARM64 wheel for Python 3.9 on macOS.

## Infrastructure

PostgreSQL runs in Docker. Start it before running the pipeline or integration tests:

```bash
docker compose up -d      # start (data persists in named volume pgdata)
docker compose down       # stop (data preserved)
docker compose down -v    # stop and wipe data
```

Credentials and connection details live in `.env` (gitignored). Copy this structure if `.env` is missing:

```
MYSQL_HOST=db.isba.co
MYSQL_PORT=3306
MYSQL_DB=basket_craft
MYSQL_USER=student
MYSQL_PASSWORD=<password>

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=basket_craft_dw
POSTGRES_USER=pipeline
POSTGRES_PASSWORD=<password>
```

## Architecture

The entire pipeline is a single file: `pipeline.py`. Three functions with clear boundaries:

- **`extract()`** — connects to MySQL (`basket_craft` DB), joins `orders → order_items → products`, returns ~40k raw row dicts. Each row has: `order_id`, `year_month` (YYYY-MM-01 string), `product_id`, `product_name`, `item_revenue`.
- **`transform(rows)`** — pure Python, no I/O. Groups by `(year_month, product_id)`, sums revenue using `decimal.Decimal` (avoids float accumulation error), counts distinct `order_id`s per group, computes average order value. Returns ~94 aggregated row dicts.
- **`load(rows)`** — connects to PostgreSQL, creates `monthly_sales` if it doesn't exist, then truncates + bulk-inserts all rows in a single transaction. Rolls back on any failure. Returns row count.
- **`main()`** — orchestrates extract → transform → load with print progress. Entry point via `if __name__ == '__main__'`.

## Tests

- `tests/test_transform.py` — 5 unit tests, no database required
- `tests/test_pipeline.py` — 7 integration tests (4 extract against live MySQL, 3 load against Docker PostgreSQL)

**Important:** Integration tests leave `monthly_sales` in a test-data state (1 row). Re-run `python pipeline.py` after running tests to restore the full 94-row dataset.

## Destination Table

```sql
SELECT year_month, product_name, order_count, revenue_usd, avg_order_value_usd
FROM monthly_sales
ORDER BY year_month DESC, revenue_usd DESC;
```

Primary key is `(year_month, product_id)`. The pipeline always does a full truncate-and-reload — there is no incremental update logic.
