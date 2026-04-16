# Basket Craft Pipeline

A Python ETL pipeline that copies data from the Basket Craft MySQL source database into an **AWS RDS PostgreSQL** instance. The pipeline runs two phases on every invocation: it aggregates monthly sales into a `monthly_sales` fact table, and copies all 8 source tables over as raw staging tables for ad-hoc analysis.

## What It Does

**Phase 1 — Aggregated sales table (`monthly_sales`):**

| Column | Description |
|---|---|
| `year_month` | First day of each month (e.g. `2026-01-01`) |
| `product_name` | One of 4 Basket Craft gift basket products |
| `order_count` | Number of distinct orders placed |
| `revenue_usd` | Gross revenue (before refunds) |
| `avg_order_value_usd` | Revenue divided by order count |

**Phase 2 — Raw table copy (8 tables, ~1.77M rows total):**

| Table | Rows |
|---|---:|
| `employees` | 20 |
| `products` | 4 |
| `order_item_refunds` | 1,731 |
| `orders` | 32,313 |
| `users` | 31,696 |
| `order_items` | 40,025 |
| `website_sessions` | 472,871 |
| `website_pageviews` | 1,188,124 |

Raw tables are copied as-is with no transformations. Schema is regenerated on every run from MySQL's current schema, so upstream column changes self-heal.

## Setup

### Prerequisites

- Python 3.9+
- Access credentials for the Basket Craft MySQL source and the AWS RDS PostgreSQL destination
- Docker is optional — only needed if you want to run PostgreSQL locally instead of against RDS

### 1. Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Create a `.env` file

```
MYSQL_HOST=db.isba.co
MYSQL_PORT=3306
MYSQL_DB=basket_craft
MYSQL_USER=student
MYSQL_PASSWORD=<password>

POSTGRES_HOST=<your-rds-endpoint>.rds.amazonaws.com
POSTGRES_PORT=5432
POSTGRES_DB=basket_craft
POSTGRES_USER=student
POSTGRES_PASSWORD=<password>
```

> **Local Docker fallback:** the included `docker-compose.yml` can run PostgreSQL on `localhost:5432` for offline dev — set `POSTGRES_HOST=localhost` and run `docker compose up -d`. The pipeline works identically against either destination.

## Running the Pipeline

```bash
.venv/bin/python3 pipeline.py
```

Expected output (~1 minute against RDS):

```
Extracting from MySQL...
  Extracted 40025 raw rows
Transforming...
  Produced 94 aggregated rows
Loading into PostgreSQL...
  Loaded 94 rows into monthly_sales
Loading raw tables into PostgreSQL...
  employees: 20 rows
  order_item_refunds: 1731 rows
  order_items: 40025 rows
  orders: 32313 rows
  products: 4 rows
  users: 31696 rows
  website_pageviews: 1188124 rows
  website_sessions: 472871 rows
Done.
```

The pipeline does a full **truncate and reload** on every run — there is no incremental update.

## Querying the Results

Connect to RDS with `psql` or any PostgreSQL client using the credentials in `.env`:

```bash
psql "host=$POSTGRES_HOST port=$POSTGRES_PORT dbname=$POSTGRES_DB user=$POSTGRES_USER"
```

Aggregated sales:

```sql
SELECT year_month, product_name, order_count, revenue_usd, avg_order_value_usd
FROM monthly_sales
ORDER BY year_month DESC, revenue_usd DESC
LIMIT 20;
```

Ad-hoc on the raw tables — e.g. top marketing channels by session count:

```sql
SELECT utm_source, utm_campaign, COUNT(*) AS sessions
FROM website_sessions
GROUP BY utm_source, utm_campaign
ORDER BY sessions DESC
LIMIT 10;
```

> **PII note:** the `users` raw table includes names, emails, addresses, and password salt/hash columns. Treat accordingly.

## Running Tests

```bash
# All tests (requires live MySQL + PostgreSQL access)
.venv/bin/pytest tests/ -v

# Unit tests only (no database required)
.venv/bin/pytest tests/test_transform.py -v
```

One integration test (`test_load_all_raw_tables_runs_and_loads_products`) copies all 8 tables and takes ~45 seconds. The rest finish in under 2 seconds combined.

> Integration tests leave `monthly_sales` in a test-data state. Re-run `python pipeline.py` after running tests to restore the full dataset.

## Project Structure

```
basket-craft-pipeline/
├── pipeline.py              # ETL: aggregated + raw-copy phases
├── docker-compose.yml       # Local PostgreSQL fallback
├── requirements.txt         # Python dependencies
├── .env                     # Credentials (gitignored)
├── docs/superpowers/
│   ├── specs/               # Feature design specs
│   └── plans/               # Step-by-step implementation plans
└── tests/
    ├── test_transform.py    # Unit tests (transform, type mapping)
    └── test_pipeline.py     # Integration tests (extract, load, raw copy)
```
