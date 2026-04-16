# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Python Environment

Use the virtual environment for all Python commands — never the system Python.

```bash
.venv/bin/python3 pipeline.py                            # run the full pipeline
.venv/bin/pytest tests/ -v                               # all tests (slow — see note below)
.venv/bin/pytest tests/test_transform.py -v              # unit tests only (no DB)
.venv/bin/pytest tests/test_pipeline.py -k "extract" -v  # filter to a test group
.venv/bin/pytest tests/test_pipeline.py::test_extract_and_load_raw_table_copies_products -v  # single test
```

Install dependencies:

```bash
python3 -m pip install -r requirements.txt
```

> **psycopg2 pin:** `psycopg2-binary==2.9.9` is intentional — 2.9.10 has no pre-built ARM64 wheel for Python 3.9 on macOS.

## Infrastructure

**Destination is AWS RDS PostgreSQL** (live host in `.env` as `POSTGRES_HOST`). The included `docker-compose.yml` predates the RDS move and is kept only as a local-dev fallback — point `POSTGRES_HOST` at `localhost` and run `docker compose up -d` to use it instead.

Credentials live in `.env` (gitignored):

```
MYSQL_HOST=db.isba.co
MYSQL_PORT=3306
MYSQL_DB=basket_craft
MYSQL_USER=student
MYSQL_PASSWORD=<password>

POSTGRES_HOST=<rds-endpoint>   # or localhost for Docker fallback
POSTGRES_PORT=5432
POSTGRES_DB=basket_craft
POSTGRES_USER=student
POSTGRES_PASSWORD=<password>
```

## Architecture

Everything lives in `pipeline.py`. The pipeline runs two distinct phases on every invocation:

### Phase 1 — Aggregated ETL (original)

- `extract()` — joins `orders → order_items → products` in MySQL, returns ~40k raw line-item dicts
- `transform(rows)` — pure Python; groups by `(year_month, product_id)`, sums revenue with `decimal.Decimal` to avoid float drift, counts distinct `order_id`s, derives AOV from totals (not per-row averages). Returns ~94 rows.
- `load(rows)` — creates `monthly_sales` if absent, `TRUNCATE` + bulk insert inside a single transaction with rollback on failure. PK: `(year_month, product_id)`.

### Phase 2 — Raw table copy

- `_mysql_to_pg_type(mysql_type)` — pure MySQL→PostgreSQL type mapper (e.g. `int unsigned` → `INTEGER`, `varchar(50)` → `VARCHAR(50)`, `decimal(6,2)` → `NUMERIC(6,2)`). Unknown types print a warning and fall back to `TEXT`.
- `extract_and_load_raw_table(mysql_conn, pg_conn, table_name)` — per-table workhorse. Uses MySQL `DESCRIBE` to reflect the schema, then does `DROP TABLE IF EXISTS` + `CREATE TABLE` + `psycopg2.extras.execute_batch` INSERT (1,000 rows/batch). Commits per table, with `try/except` + `rollback()` to avoid poisoning the shared connection when one table fails.
- `load_all_raw_tables()` — orchestrator. Opens one MySQL and one PostgreSQL connection, loops over `TABLES_TO_COPY` (8 tables defined at module top), prints per-table counts, closes both in `finally`.

`main()` calls Phase 1 then Phase 2. If Phase 1 produces zero rows, it aborts before touching PostgreSQL.

### Why DROP+CREATE, not CREATE IF NOT EXISTS + TRUNCATE

Every raw-copy run rebuilds the destination schema from the current MySQL schema. This self-heals when columns are added/renamed/retyped upstream. See `docs/superpowers/specs/2026-04-15-raw-table-copy-design.md` (Amendments section) for the rationale.

## Destination Tables

After a full run, PostgreSQL has 9 tables (~1.77M rows total):

| Table | Source | Rows |
|---|---|---:|
| `monthly_sales` | aggregated (`transform()`) | 94 |
| `employees` | raw copy | 20 |
| `order_item_refunds` | raw copy | 1,731 |
| `order_items` | raw copy | 40,025 |
| `orders` | raw copy | 32,313 |
| `products` | raw copy | 4 |
| `users` | raw copy | 31,696 |
| `website_sessions` | raw copy | 472,871 |
| `website_pageviews` | raw copy | 1,188,124 |

The raw tables are plain heap tables — no indexes, no PKs. They're read targets for ad-hoc queries, not OLTP.

> **PII in `users`:** the `users` table includes names, emails, billing/shipping addresses, and password salt/hash. Treat accordingly.

## Tests

- `tests/test_transform.py` — 7 unit tests; no database needed. Covers `transform()` and `_mysql_to_pg_type()`.
- `tests/test_pipeline.py` — 9 integration tests against live MySQL + live PostgreSQL. Includes `test_load_all_raw_tables_runs_and_loads_products`, which copies all 8 tables and takes **~45 seconds against RDS** (the slow one).

**Test side-effect note:** Running the integration tests leaves `monthly_sales` in a 1-row test state and overwrites the 8 raw tables with fresh copies of MySQL. Re-run `pipeline.py` after a test run if you want the full 94-row `monthly_sales` back.

## Design Docs

- `docs/superpowers/specs/` — design specs per feature
- `docs/superpowers/plans/` — step-by-step implementation plans

Check these before starting non-trivial changes; most decisions are documented there with "why", not just "what".
