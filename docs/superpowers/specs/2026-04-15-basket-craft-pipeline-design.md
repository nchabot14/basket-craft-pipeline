# Basket Craft Pipeline — Design Spec

**Date:** 2026-04-15
**Author:** Nick Chabot
**Status:** Approved

---

## Overview

A monthly sales ETL pipeline that extracts transaction data from the Basket Craft MySQL database, aggregates it into a `monthly_sales` fact table, and loads it into a local PostgreSQL instance running in Docker. The destination table powers a monthly sales dashboard showing revenue, order counts, and average order value by product and month.

---

## Architecture

```
SOURCE (MySQL)                 TRANSFORM              DESTINATION (PostgreSQL)
db.isba.co:3306                Python ETL             Docker :5432
basket_craft DB                                       basket_craft_dw DB

┌──────────────┐               ┌──────────────────┐   ┌──────────────────────┐
│  orders      │──┐            │                  │   │                      │
│  order_id    │  │  EXTRACT   │  1. Join tables  │   │  monthly_sales       │
│  created_at  │  ├──────────▶ │  2. Group by     │──▶│  ─────────────────── │
│  price_usd   │  │  pymysql   │     month +      │   │  year_month          │
└──────────────┘  │            │     product      │   │  product_id          │
                  │            │  3. Aggregate:   │   │  product_name        │
┌──────────────┐  │            │     - revenue    │   │  order_count         │
│  order_items │──┤            │     - orders     │   │  revenue_usd         │
│  order_id    │  │            │     - avg AOV    │   │  avg_order_value_usd │
│  product_id  │  │            │                  │   │  loaded_at           │
│  price_usd   │  │            └──────────────────┘   └──────────────────────┘
└──────────────┘  │
                  │
┌──────────────┐  │
│  products    │──┘
│  product_id  │
│  product_name│
└──────────────┘
```

---

## Source Schema

**Database:** `basket_craft` on `db.isba.co:3306`

| Table | Relevant Columns |
|---|---|
| `orders` | `order_id`, `created_at`, `price_usd` |
| `order_items` | `order_id`, `product_id`, `price_usd` |
| `products` | `product_id`, `product_name` |

**Products (4 total — used as categories):**

| ID | Name |
|----|------|
| 1  | The Original Gift Basket |
| 2  | The Valentine's Gift Basket |
| 3  | The Birthday Gift Basket |
| 4  | The Holiday Gift Basket |

**Data volume:** ~32,300 orders, March 2023 – March 2026.

**Revenue definition:** Gross (refunds from `order_item_refunds` are excluded).

---

## Extraction

One SQL query joins the three source tables and returns raw line-item rows:

```sql
SELECT
    o.order_id,
    DATE_FORMAT(o.created_at, '%Y-%m-01')  AS year_month,
    p.product_id,
    p.product_name,
    oi.price_usd                            AS item_revenue
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products    p  ON oi.product_id = p.product_id
ORDER BY year_month, p.product_id
```

Joining via `order_items` (not `orders.price_usd` directly) ensures revenue is attributed per product on multi-item orders.

---

## Transformation

Python groups raw rows by `(year_month, product_id, product_name)` and computes:

- `revenue_usd` = `sum(item_revenue)`
- `order_count` = `count(distinct order_id)`
- `avg_order_value_usd` = `revenue_usd / order_count`

Average order value is derived from totals (not averaged directly) so it reflects per-transaction spend, not per-line-item spend.

Expected output: ~144 rows (4 products × ~36 months).

---

## Destination

**Database:** `basket_craft_dw` on PostgreSQL in Docker (`localhost:5432`)

**Destination table:**

```sql
CREATE TABLE IF NOT EXISTS monthly_sales (
    year_month            DATE           NOT NULL,
    product_id            INTEGER        NOT NULL,
    product_name          VARCHAR(50)    NOT NULL,
    order_count           INTEGER        NOT NULL,
    revenue_usd           NUMERIC(12,2)  NOT NULL,
    avg_order_value_usd   NUMERIC(10,2)  NOT NULL,
    loaded_at             TIMESTAMP      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (year_month, product_id)
);
```

**Load strategy — truncate and reload inside a transaction:**

```
1. BEGIN transaction
2. TRUNCATE monthly_sales
3. INSERT all transformed rows
4. COMMIT
```

The transaction ensures the table is never in a partially-loaded state. A failed insert rolls back, preserving the previous data.

---

## Infrastructure

**`docker-compose.yml`:**

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: basket_craft_dw
      POSTGRES_USER: pipeline
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

volumes:
  pgdata:
```

Start with: `docker compose up -d`

**`.env` (gitignored):**

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

---

## Project Structure

```
basket-craft-pipeline/
├── .env                  # All credentials (gitignored)
├── docker-compose.yml    # PostgreSQL container
├── pipeline.py           # Main ETL script
│   ├── extract()         # Queries MySQL, returns raw rows
│   ├── transform()       # Groups and aggregates rows
│   └── load()            # Truncates + inserts into PostgreSQL
└── requirements.txt      # pymysql, psycopg2-binary, python-dotenv
```

---

## Error Handling

| Failure | Behavior |
|---|---|
| MySQL connection fails | Raise immediately — no partial work |
| Transform produces 0 rows | Abort before touching PostgreSQL — prevents wiping table with empty data |
| PostgreSQL load fails mid-insert | Transaction rolls back — old data preserved |

Post-load validation:

```python
cursor.execute("SELECT COUNT(*) FROM monthly_sales")
assert cursor.fetchone()[0] > 0, "Load produced empty table — aborting"
```

---

## Testing

1. **Smoke test:** Run `python pipeline.py` against the real database. Confirm ~144 rows load successfully.
2. **Spot-check query:**

```sql
SELECT year_month, product_name, order_count, revenue_usd, avg_order_value_usd
FROM monthly_sales
ORDER BY year_month DESC, revenue_usd DESC
LIMIT 10;
```

No unit tests — an integration test against the real database provides more confidence than mocking at this scale.
