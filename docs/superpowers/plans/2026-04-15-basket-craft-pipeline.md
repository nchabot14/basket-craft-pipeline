# Basket Craft Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Python ETL pipeline that extracts monthly sales data from a MySQL database, aggregates it by product and month, and loads it into a local PostgreSQL instance running in Docker.

**Architecture:** A single `pipeline.py` script with three clearly separated functions — `extract()` queries MySQL, `transform()` aggregates raw rows in Python, and `load()` truncates and reloads the PostgreSQL `monthly_sales` fact table inside a transaction.

**Tech Stack:** Python 3, pymysql, psycopg2-binary, python-dotenv, pytest, Docker (postgres:16)

---

## File Map

| File | Responsibility |
|---|---|
| `.env` | All credentials — MySQL source + PostgreSQL destination |
| `docker-compose.yml` | PostgreSQL container definition |
| `requirements.txt` | Python dependencies |
| `pipeline.py` | ETL script: `extract()`, `transform()`, `load()`, `main()` |
| `tests/test_transform.py` | Unit tests for `transform()` — no external deps |
| `tests/test_pipeline.py` | Integration tests for `extract()` and `load()` |

---

## Task 1: Project Scaffolding

**Files:**
- Create: `.env`
- Create: `docker-compose.yml`
- Create: `requirements.txt`
- Create: `tests/__init__.py`

- [ ] **Step 1: Create `.env`**

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

- [ ] **Step 2: Create `docker-compose.yml`**

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

- [ ] **Step 3: Create `requirements.txt`**

```
pymysql==1.1.1
psycopg2-binary==2.9.10
python-dotenv==1.1.0
pytest==8.3.5
```

- [ ] **Step 4: Install dependencies**

```bash
pip install -r requirements.txt
```

Expected: all packages install without errors.

- [ ] **Step 5: Start PostgreSQL container**

```bash
docker compose up -d
```

Expected output includes:
```
✔ Container basket-craft-pipeline-postgres-1  Started
```

- [ ] **Step 6: Verify MySQL connection**

```bash
python3 -c "
import pymysql, os
from dotenv import load_dotenv
load_dotenv()
conn = pymysql.connect(host=os.environ['MYSQL_HOST'], port=int(os.environ['MYSQL_PORT']),
    database=os.environ['MYSQL_DB'], user=os.environ['MYSQL_USER'], password=os.environ['MYSQL_PASSWORD'])
print('MySQL OK')
conn.close()
"
```

Expected: `MySQL OK`

- [ ] **Step 7: Verify PostgreSQL connection**

```bash
python3 -c "
import psycopg2, os
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(host=os.environ['POSTGRES_HOST'], port=int(os.environ['POSTGRES_PORT']),
    dbname=os.environ['POSTGRES_DB'], user=os.environ['POSTGRES_USER'], password=os.environ['POSTGRES_PASSWORD'])
print('PostgreSQL OK')
conn.close()
"
```

Expected: `PostgreSQL OK`

- [ ] **Step 8: Create `tests/__init__.py`**

```python
```

(Empty file — marks `tests/` as a Python package so pytest discovers tests correctly.)

- [ ] **Step 9: Commit**

```bash
git add .env docker-compose.yml requirements.txt tests/__init__.py
git commit -m "feat: scaffold project dependencies and infrastructure"
```

---

## Task 2: Implement `transform()`

**Files:**
- Create: `pipeline.py`
- Create: `tests/test_transform.py`

`transform()` takes a list of raw row dicts from MySQL and returns a list of aggregated dicts grouped by `(year_month, product_id)`.

- [ ] **Step 1: Create `tests/test_transform.py` with failing tests**

```python
from pipeline import transform


def test_transform_aggregates_revenue_and_order_count():
    rows = [
        {'order_id': 1, 'year_month': '2024-01-01', 'product_id': 1,
         'product_name': 'The Original Gift Basket', 'item_revenue': 49.99},
        {'order_id': 2, 'year_month': '2024-01-01', 'product_id': 1,
         'product_name': 'The Original Gift Basket', 'item_revenue': 49.99},
    ]
    result = transform(rows)
    assert len(result) == 1
    row = result[0]
    assert row['year_month'] == '2024-01-01'
    assert row['product_id'] == 1
    assert row['product_name'] == 'The Original Gift Basket'
    assert row['order_count'] == 2
    assert row['revenue_usd'] == 99.98
    assert row['avg_order_value_usd'] == 49.99


def test_transform_splits_by_product():
    rows = [
        {'order_id': 1, 'year_month': '2024-01-01', 'product_id': 1,
         'product_name': 'The Original Gift Basket', 'item_revenue': 49.99},
        {'order_id': 1, 'year_month': '2024-01-01', 'product_id': 2,
         'product_name': "The Valentine's Gift Basket", 'item_revenue': 59.99},
    ]
    result = transform(rows)
    assert len(result) == 2
    product_ids = {r['product_id'] for r in result}
    assert product_ids == {1, 2}


def test_transform_splits_by_month():
    rows = [
        {'order_id': 1, 'year_month': '2024-01-01', 'product_id': 1,
         'product_name': 'The Original Gift Basket', 'item_revenue': 49.99},
        {'order_id': 2, 'year_month': '2024-02-01', 'product_id': 1,
         'product_name': 'The Original Gift Basket', 'item_revenue': 49.99},
    ]
    result = transform(rows)
    assert len(result) == 2
    months = {r['year_month'] for r in result}
    assert months == {'2024-01-01', '2024-02-01'}


def test_transform_deduplicates_order_ids():
    # Same order_id appears twice (multi-item order) — should count as 1 order
    rows = [
        {'order_id': 1, 'year_month': '2024-01-01', 'product_id': 1,
         'product_name': 'The Original Gift Basket', 'item_revenue': 49.99},
        {'order_id': 1, 'year_month': '2024-01-01', 'product_id': 1,
         'product_name': 'The Original Gift Basket', 'item_revenue': 49.99},
    ]
    result = transform(rows)
    assert result[0]['order_count'] == 1
    assert result[0]['revenue_usd'] == 99.98
    assert result[0]['avg_order_value_usd'] == 99.98


def test_transform_empty_input():
    assert transform([]) == []
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_transform.py -v
```

Expected: `ImportError` — `pipeline` module does not exist yet.

- [ ] **Step 3: Create `pipeline.py` with `transform()`**

```python
from collections import defaultdict


def transform(rows):
    """
    Group raw order-item rows by (year_month, product_id) and aggregate:
      - revenue_usd: sum of item_revenue across all line items
      - order_count: count of distinct order_ids (not line items)
      - avg_order_value_usd: revenue_usd / order_count

    Args:
        rows: list of dicts with keys:
              order_id, year_month, product_id, product_name, item_revenue

    Returns:
        list of dicts with keys:
        year_month, product_id, product_name, order_count,
        revenue_usd, avg_order_value_usd
    """
    if not rows:
        return []

    groups = defaultdict(lambda: {
        'product_name': '',
        'order_ids': set(),
        'revenue': 0.0,
    })

    for row in rows:
        key = (row['year_month'], row['product_id'])
        groups[key]['product_name'] = row['product_name']
        groups[key]['order_ids'].add(row['order_id'])
        groups[key]['revenue'] += float(row['item_revenue'])

    result = []
    for (year_month, product_id), data in sorted(groups.items()):
        order_count = len(data['order_ids'])
        revenue = round(data['revenue'], 2)
        result.append({
            'year_month': year_month,
            'product_id': product_id,
            'product_name': data['product_name'],
            'order_count': order_count,
            'revenue_usd': revenue,
            'avg_order_value_usd': round(revenue / order_count, 2),
        })
    return result
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_transform.py -v
```

Expected: 5 tests, all PASSED.

- [ ] **Step 5: Commit**

```bash
git add pipeline.py tests/test_transform.py
git commit -m "feat: implement transform() with unit tests"
```

---

## Task 3: Implement `extract()`

**Files:**
- Modify: `pipeline.py` — add `extract()`
- Create: `tests/test_pipeline.py` — integration test for `extract()`

`extract()` connects to MySQL, runs the join query, and returns a list of raw row dicts.

- [ ] **Step 1: Write failing integration test in `tests/test_pipeline.py`**

```python
import pytest
from pipeline import extract


def test_extract_returns_rows():
    rows = extract()
    assert len(rows) > 0


def test_extract_row_shape():
    rows = extract()
    first = rows[0]
    assert 'order_id' in first
    assert 'year_month' in first
    assert 'product_id' in first
    assert 'product_name' in first
    assert 'item_revenue' in first


def test_extract_year_month_format():
    rows = extract()
    # year_month must be a string like '2024-01-01' (first day of month)
    for row in rows[:10]:
        assert row['year_month'].endswith('-01'), \
            f"Expected year_month to end in '-01', got: {row['year_month']}"


def test_extract_known_products():
    rows = extract()
    product_names = {row['product_name'] for row in rows}
    assert 'The Original Gift Basket' in product_names
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_pipeline.py::test_extract_returns_rows -v
```

Expected: `ImportError` — `extract` not defined in `pipeline`.

- [ ] **Step 3: Add `extract()` to `pipeline.py`**

Add these imports at the top of `pipeline.py`:

```python
import os
import pymysql
import pymysql.cursors
from dotenv import load_dotenv

load_dotenv()
```

Add this function to `pipeline.py`:

```python
def extract():
    """
    Connect to MySQL and return raw order-item rows joined with orders and products.

    Returns:
        list of dicts with keys:
        order_id, year_month (YYYY-MM-01 string), product_id, product_name, item_revenue
    """
    conn = pymysql.connect(
        host=os.environ['MYSQL_HOST'],
        port=int(os.environ['MYSQL_PORT']),
        database=os.environ['MYSQL_DB'],
        user=os.environ['MYSQL_USER'],
        password=os.environ['MYSQL_PASSWORD'],
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT
                    o.order_id,
                    DATE_FORMAT(o.created_at, '%%Y-%%m-01') AS year_month,
                    p.product_id,
                    p.product_name,
                    oi.price_usd                            AS item_revenue
                FROM orders o
                JOIN order_items oi ON o.order_id = oi.order_id
                JOIN products    p  ON oi.product_id = p.product_id
                ORDER BY year_month, p.product_id
            """)
            return cursor.fetchall()
    finally:
        conn.close()
```

- [ ] **Step 4: Run integration tests to verify they pass**

```bash
pytest tests/test_pipeline.py -k "extract" -v
```

Expected: 4 tests, all PASSED.

- [ ] **Step 5: Commit**

```bash
git add pipeline.py tests/test_pipeline.py
git commit -m "feat: implement extract() with integration tests"
```

---

## Task 4: Implement `load()`

**Files:**
- Modify: `pipeline.py` — add `load()`
- Modify: `tests/test_pipeline.py` — add integration tests for `load()`

`load()` creates the `monthly_sales` table if needed, truncates it, inserts all rows inside a transaction, and returns the row count. Rolls back on failure.

- [ ] **Step 1: Add failing integration tests to `tests/test_pipeline.py`**

Append to `tests/test_pipeline.py`:

```python
import psycopg2
from pipeline import load


def _pg_conn():
    return psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        port=int(os.environ['POSTGRES_PORT']),
        dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
    )


def test_load_inserts_rows():
    test_rows = [
        {
            'year_month': '2024-01-01',
            'product_id': 1,
            'product_name': 'The Original Gift Basket',
            'order_count': 10,
            'revenue_usd': 499.90,
            'avg_order_value_usd': 49.99,
        }
    ]
    count = load(test_rows)
    assert count == 1


def test_load_replaces_previous_data():
    batch_a = [
        {
            'year_month': '2024-01-01',
            'product_id': 1,
            'product_name': 'The Original Gift Basket',
            'order_count': 5,
            'revenue_usd': 249.95,
            'avg_order_value_usd': 49.99,
        }
    ]
    batch_b = [
        {
            'year_month': '2024-02-01',
            'product_id': 2,
            'product_name': "The Valentine's Gift Basket",
            'order_count': 3,
            'revenue_usd': 179.97,
            'avg_order_value_usd': 59.99,
        }
    ]
    load(batch_a)
    count = load(batch_b)
    # Table should contain only batch_b — batch_a was truncated
    assert count == 1
    conn = _pg_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT product_id FROM monthly_sales")
        product_ids = [row[0] for row in cur.fetchall()]
    conn.close()
    assert product_ids == [2]


def test_load_raises_on_empty_rows():
    with pytest.raises(AssertionError, match="empty table"):
        load([])
```

Also add `import os` to the top of `tests/test_pipeline.py` if not already present.

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_pipeline.py -k "load" -v
```

Expected: `ImportError` — `load` not defined in `pipeline`.

- [ ] **Step 3: Add `load()` to `pipeline.py`**

Add this import at the top of `pipeline.py`:

```python
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
```

Add this function to `pipeline.py`:

```python
def load(rows):
    """
    Truncate monthly_sales and insert all rows inside a single transaction.
    Creates the table if it doesn't exist.
    Rolls back and re-raises on any failure.

    Args:
        rows: list of dicts with keys:
              year_month, product_id, product_name,
              order_count, revenue_usd, avg_order_value_usd

    Returns:
        int — number of rows loaded

    Raises:
        AssertionError if 0 rows would be loaded (guard against wiping table)
    """
    assert rows, "Load produced empty table — aborting to protect destination"

    conn = psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        port=int(os.environ['POSTGRES_PORT']),
        dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS monthly_sales (
                    year_month            DATE           NOT NULL,
                    product_id            INTEGER        NOT NULL,
                    product_name          VARCHAR(50)    NOT NULL,
                    order_count           INTEGER        NOT NULL,
                    revenue_usd           NUMERIC(12,2)  NOT NULL,
                    avg_order_value_usd   NUMERIC(10,2)  NOT NULL,
                    loaded_at             TIMESTAMP      NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (year_month, product_id)
                )
            """)
            cursor.execute("TRUNCATE monthly_sales")
            loaded_at = datetime.now(timezone.utc)
            psycopg2.extras.execute_batch(cursor, """
                INSERT INTO monthly_sales
                    (year_month, product_id, product_name,
                     order_count, revenue_usd, avg_order_value_usd, loaded_at)
                VALUES
                    (%(year_month)s, %(product_id)s, %(product_name)s,
                     %(order_count)s, %(revenue_usd)s, %(avg_order_value_usd)s,
                     %(loaded_at)s)
            """, [{**row, 'loaded_at': loaded_at} for row in rows])
            cursor.execute("SELECT COUNT(*) FROM monthly_sales")
            count = cursor.fetchone()[0]
            assert count > 0, "Load produced empty table — aborting"
        conn.commit()
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_pipeline.py -k "load" -v
```

Expected: 3 tests, all PASSED.

- [ ] **Step 5: Commit**

```bash
git add pipeline.py tests/test_pipeline.py
git commit -m "feat: implement load() with integration tests"
```

---

## Task 5: Wire `main()` with Error Handling

**Files:**
- Modify: `pipeline.py` — add `main()`

`main()` orchestrates extract → transform → load with clear progress output and an early exit guard if extraction returns 0 rows.

- [ ] **Step 1: Add `main()` to `pipeline.py`**

Append to `pipeline.py`:

```python
def main():
    print("Extracting from MySQL...")
    rows = extract()
    print(f"  Extracted {len(rows)} raw rows")

    if not rows:
        raise RuntimeError("No rows extracted — aborting to protect destination table")

    print("Transforming...")
    aggregated = transform(rows)
    print(f"  Produced {len(aggregated)} aggregated rows")

    print("Loading into PostgreSQL...")
    count = load(aggregated)
    print(f"  Loaded {count} rows into monthly_sales")
    print("Done.")


if __name__ == '__main__':
    main()
```

- [ ] **Step 2: Commit**

```bash
git add pipeline.py
git commit -m "feat: add main() with extract/transform/load orchestration"
```

---

## Task 6: End-to-End Smoke Test and Spot-Check

**Files:**
- No new files — run the pipeline and verify output.

- [ ] **Step 1: Run the full pipeline**

```bash
python3 pipeline.py
```

Expected output:
```
Extracting from MySQL...
  Extracted NNNNN raw rows
Transforming...
  Produced NNN aggregated rows
Loading into PostgreSQL...
  Loaded NNN rows into monthly_sales
Done.
```

- Extracted rows should be in the range of 30,000–40,000.
- Aggregated rows should be approximately 144 (4 products × ~36 months).

- [ ] **Step 2: Run spot-check query against PostgreSQL**

```bash
docker exec -it basket-craft-pipeline-postgres-1 psql -U pipeline -d basket_craft_dw -c "
SELECT year_month, product_name, order_count, revenue_usd, avg_order_value_usd
FROM monthly_sales
ORDER BY year_month DESC, revenue_usd DESC
LIMIT 10;
"
```

Expected: 10 rows with plausible values — nonzero revenue, order_count > 0, avg_order_value_usd between \$30–\$150.

- [ ] **Step 3: Verify row count**

```bash
docker exec -it basket-craft-pipeline-postgres-1 psql -U pipeline -d basket_craft_dw -c "
SELECT COUNT(*) FROM monthly_sales;
"
```

Expected: approximately 144 rows.

- [ ] **Step 4: Run the full test suite**

```bash
pytest tests/ -v
```

Expected: all tests PASSED (5 unit + 7 integration).

- [ ] **Step 5: Commit**

```bash
git add .
git commit -m "feat: verified end-to-end pipeline — monthly_sales populated"
```
