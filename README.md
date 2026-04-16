# Basket Craft Pipeline

A Python ETL pipeline that extracts monthly sales data from the Basket Craft MySQL database, aggregates it by product and month, and loads it into a local PostgreSQL instance for dashboard reporting.

## What It Does

The pipeline produces a `monthly_sales` fact table with three metrics per product per month:

| Column | Description |
|---|---|
| `year_month` | First day of each month (e.g. `2026-01-01`) |
| `product_name` | One of 4 Basket Craft gift basket products |
| `order_count` | Number of distinct orders placed |
| `revenue_usd` | Gross revenue (before refunds) |
| `avg_order_value_usd` | Revenue divided by order count |

## Setup

### Prerequisites

- Python 3.9+
- Docker

### 1. Create a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Create a `.env` file

```bash
cp .env.example .env   # if available, or create manually
```

`.env` contents:

```
MYSQL_HOST=db.isba.co
MYSQL_PORT=3306
MYSQL_DB=basket_craft
MYSQL_USER=student
MYSQL_PASSWORD=learn_sql

POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=basket_craft_dw
POSTGRES_USER=pipeline
POSTGRES_PASSWORD=pipeline
```

### 3. Start PostgreSQL

```bash
docker compose up -d
```

The container exposes PostgreSQL on `localhost:5432`. Data persists in a named Docker volume (`pgdata`) across restarts.

## Running the Pipeline

```bash
.venv/bin/python3 pipeline.py
```

Expected output:

```
Extracting from MySQL...
  Extracted 40025 raw rows
Transforming...
  Produced 94 aggregated rows
Loading into PostgreSQL...
  Loaded 94 rows into monthly_sales
Done.
```

The pipeline does a full **truncate and reload** on every run — there is no incremental update.

## Querying the Results

```bash
docker exec -it basket-craft-pipeline-postgres-1 psql -U pipeline -d basket_craft_dw
```

```sql
SELECT year_month, product_name, order_count, revenue_usd, avg_order_value_usd
FROM monthly_sales
ORDER BY year_month DESC, revenue_usd DESC
LIMIT 20;
```

## Running Tests

```bash
# All tests (requires MySQL + Docker PostgreSQL running)
.venv/bin/pytest tests/ -v

# Unit tests only (no database required)
.venv/bin/pytest tests/test_transform.py -v
```

> Integration tests leave `monthly_sales` in a test-data state. Re-run `python pipeline.py` after running tests to restore the full dataset.

## Project Structure

```
basket-craft-pipeline/
├── pipeline.py          # ETL script: extract(), transform(), load(), main()
├── docker-compose.yml   # PostgreSQL container
├── requirements.txt     # Python dependencies
├── .env                 # Credentials (gitignored)
└── tests/
    ├── test_transform.py    # Unit tests for transform()
    └── test_pipeline.py     # Integration tests for extract() and load()
```
