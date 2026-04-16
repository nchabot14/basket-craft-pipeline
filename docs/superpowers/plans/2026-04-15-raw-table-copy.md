# Raw Table Copy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `pipeline.py` to copy all 8 raw MySQL tables into PostgreSQL as-is (no transformations), using truncate-and-reload on each run.

**Architecture:** Three new functions are added to `pipeline.py`: a pure type-mapping helper (`_mysql_to_pg_type`), a per-table copy function (`extract_and_load_raw_table`), and an orchestrator (`load_all_raw_tables`) that opens both DB connections, loops over 8 tables, and closes. `main()` calls `load_all_raw_tables()` after the existing ETL step.

**Tech Stack:** Python 3.9, `pymysql` (MySQL reads), `psycopg2` + `psycopg2.extras` (PostgreSQL writes), `python-dotenv` (credentials). No new dependencies.

---

## File Map

| File | Change |
|---|---|
| `pipeline.py` | Add `TABLES_TO_COPY`, `_mysql_to_pg_type`, `extract_and_load_raw_table`, `load_all_raw_tables`; update `main()` |
| `tests/test_transform.py` | Add 2 unit tests for `_mysql_to_pg_type` |
| `tests/test_pipeline.py` | Add 2 integration tests for `extract_and_load_raw_table` and `load_all_raw_tables` |

---

## Task 1: `_mysql_to_pg_type` — type mapping helper

**Files:**
- Modify: `pipeline.py` (add function after imports, before `extract`)
- Modify: `tests/test_transform.py` (add 2 tests at end of file)

- [ ] **Step 1: Write the failing tests**

Add to the bottom of `tests/test_transform.py`:

```python
from pipeline import _mysql_to_pg_type


def test_mysql_to_pg_type_maps_common_types():
    assert _mysql_to_pg_type('int') == 'INTEGER'
    assert _mysql_to_pg_type('int unsigned') == 'INTEGER'
    assert _mysql_to_pg_type('smallint unsigned') == 'SMALLINT'
    assert _mysql_to_pg_type('varchar(50)') == 'VARCHAR(50)'
    assert _mysql_to_pg_type('decimal(6,2)') == 'NUMERIC(6,2)'
    assert _mysql_to_pg_type('decimal(10,2)') == 'NUMERIC(10,2)'
    assert _mysql_to_pg_type('timestamp') == 'TIMESTAMP'
    assert _mysql_to_pg_type('text') == 'TEXT'


def test_mysql_to_pg_type_unknown_falls_back_to_text():
    assert _mysql_to_pg_type('blob') == 'TEXT'
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/test_transform.py::test_mysql_to_pg_type_maps_common_types tests/test_transform.py::test_mysql_to_pg_type_unknown_falls_back_to_text -v
```

Expected: `FAILED` with `ImportError: cannot import name '_mysql_to_pg_type'`

- [ ] **Step 3: Implement `_mysql_to_pg_type` in `pipeline.py`**

Add this block immediately after the `load_dotenv()` line and before `def extract()`:

```python
TABLES_TO_COPY = [
    'employees',
    'order_item_refunds',
    'order_items',
    'orders',
    'products',
    'users',
    'website_pageviews',
    'website_sessions',
]


def _mysql_to_pg_type(mysql_type):
    t = mysql_type.lower()
    if t.startswith('int'):
        return 'INTEGER'
    if t.startswith('smallint'):
        return 'SMALLINT'
    if t.startswith('varchar'):
        return 'VARCHAR' + t[len('varchar'):]
    if t.startswith('decimal'):
        return 'NUMERIC' + t[len('decimal'):]
    if t == 'timestamp':
        return 'TIMESTAMP'
    if t == 'text':
        return 'TEXT'
    print(f'Warning: unknown MySQL type "{mysql_type}", using TEXT')
    return 'TEXT'
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/test_transform.py -v
```

Expected: all 7 tests `PASSED`

- [ ] **Step 5: Commit**

```bash
git add pipeline.py tests/test_transform.py
git commit -m "feat: add _mysql_to_pg_type helper and TABLES_TO_COPY constant"
```

---

## Task 2: `extract_and_load_raw_table` — per-table copy

**Files:**
- Modify: `pipeline.py` (add function after `load`)
- Modify: `tests/test_pipeline.py` (add 1 integration test)

- [ ] **Step 1: Write the failing integration test**

Add to the bottom of `tests/test_pipeline.py`:

```python
def test_extract_and_load_raw_table_copies_products():
    from pipeline import extract_and_load_raw_table
    import pymysql
    import pymysql.cursors
    from dotenv import load_dotenv
    load_dotenv()

    mysql_conn = pymysql.connect(
        host=os.environ['MYSQL_HOST'],
        port=int(os.environ['MYSQL_PORT']),
        database=os.environ['MYSQL_DB'],
        user=os.environ['MYSQL_USER'],
        password=os.environ['MYSQL_PASSWORD'],
        cursorclass=pymysql.cursors.DictCursor,
    )
    pg_conn = psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        port=int(os.environ['POSTGRES_PORT']),
        dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
    )
    try:
        count = extract_and_load_raw_table(mysql_conn, pg_conn, 'products')
        assert count == 4
        with pg_conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM "products"')
            assert cur.fetchone()[0] == 4
    finally:
        mysql_conn.close()
        pg_conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

```bash
.venv/bin/pytest tests/test_pipeline.py::test_extract_and_load_raw_table_copies_products -v
```

Expected: `FAILED` with `ImportError: cannot import name 'extract_and_load_raw_table'`

- [ ] **Step 3: Implement `extract_and_load_raw_table` in `pipeline.py`**

Add this function after `load()` and before `main()`:

```python
def extract_and_load_raw_table(mysql_conn, pg_conn, table_name):
    """
    Copy one MySQL table into PostgreSQL as-is.
    Creates the table if it doesn't exist, truncates it, then bulk-inserts all rows.
    Commits after each table. Returns the number of rows inserted.
    """
    with mysql_conn.cursor() as cur:
        cur.execute(f'DESCRIBE `{table_name}`')
        columns = cur.fetchall()
        cur.execute(f'SELECT * FROM `{table_name}`')
        rows = cur.fetchall()

    col_names = [c['Field'] for c in columns]
    col_defs = ', '.join(
        f'"{c["Field"]}" {_mysql_to_pg_type(c["Type"])}'
        for c in columns
    )

    with pg_conn.cursor() as cur:
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})')
        cur.execute(f'TRUNCATE "{table_name}"')
        if rows:
            col_list = ', '.join(f'"{name}"' for name in col_names)
            placeholders = ', '.join(f'%({name})s' for name in col_names)
            psycopg2.extras.execute_batch(
                cur,
                f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders})',
                rows,
                page_size=1000,
            )
    pg_conn.commit()
    return len(rows)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
.venv/bin/pytest tests/test_pipeline.py::test_extract_and_load_raw_table_copies_products -v
```

Expected: `PASSED`

- [ ] **Step 5: Commit**

```bash
git add pipeline.py tests/test_pipeline.py
git commit -m "feat: add extract_and_load_raw_table for per-table MySQL→PostgreSQL copy"
```

---

## Task 3: `load_all_raw_tables` — orchestrator

**Files:**
- Modify: `pipeline.py` (add function after `extract_and_load_raw_table`)
- Modify: `tests/test_pipeline.py` (add 1 integration test)

> **Note:** This test copies all 8 tables including `website_pageviews` (1.2M rows) and `website_sessions` (473K rows). Expect it to take 2–5 minutes depending on network latency to RDS.

- [ ] **Step 1: Write the failing integration test**

Add to the bottom of `tests/test_pipeline.py`:

```python
def test_load_all_raw_tables_runs_and_loads_products():
    # Copies all 8 tables — takes 2-5 minutes due to website_pageviews (1.2M rows)
    from pipeline import load_all_raw_tables
    load_all_raw_tables()  # should not raise
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM "products"')
            assert cur.fetchone()[0] == 4
    finally:
        conn.close()
```

- [ ] **Step 2: Run test to verify it fails**

```bash
.venv/bin/pytest tests/test_pipeline.py::test_load_all_raw_tables_runs_and_loads_products -v
```

Expected: `FAILED` with `ImportError: cannot import name 'load_all_raw_tables'`

- [ ] **Step 3: Implement `load_all_raw_tables` in `pipeline.py`**

Add this function after `extract_and_load_raw_table` and before `main()`:

```python
def load_all_raw_tables():
    """
    Copy all raw MySQL tables into PostgreSQL.
    Opens both connections once, loops over TABLES_TO_COPY, commits per table.
    """
    mysql_conn = pymysql.connect(
        host=os.environ['MYSQL_HOST'],
        port=int(os.environ['MYSQL_PORT']),
        database=os.environ['MYSQL_DB'],
        user=os.environ['MYSQL_USER'],
        password=os.environ['MYSQL_PASSWORD'],
        cursorclass=pymysql.cursors.DictCursor,
    )
    pg_conn = psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        port=int(os.environ['POSTGRES_PORT']),
        dbname=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD'],
    )
    try:
        for table in TABLES_TO_COPY:
            count = extract_and_load_raw_table(mysql_conn, pg_conn, table)
            print(f'  {table}: {count} rows')
    finally:
        mysql_conn.close()
        pg_conn.close()
```

- [ ] **Step 4: Run test to verify it passes**

```bash
.venv/bin/pytest tests/test_pipeline.py::test_load_all_raw_tables_runs_and_loads_products -v
```

Expected: `PASSED` (after 2–5 minutes)

- [ ] **Step 5: Commit**

```bash
git add pipeline.py tests/test_pipeline.py
git commit -m "feat: add load_all_raw_tables orchestrator"
```

---

## Task 4: Update `main()` and smoke test

**Files:**
- Modify: `pipeline.py` (update `main()`)

- [ ] **Step 1: Update `main()` in `pipeline.py`**

Replace the existing `main()` body with:

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

    print("Loading raw tables into PostgreSQL...")
    load_all_raw_tables()
    print("Done.")
```

- [ ] **Step 2: Run the full pipeline**

```bash
.venv/bin/python3 pipeline.py
```

Expected output (approximate):
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

- [ ] **Step 3: Spot-check raw tables in PostgreSQL**

```bash
.venv/bin/python3 -c "
import os, psycopg2
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(
    host=os.environ['POSTGRES_HOST'], port=os.environ['POSTGRES_PORT'],
    dbname=os.environ['POSTGRES_DB'], user=os.environ['POSTGRES_USER'],
    password=os.environ['POSTGRES_PASSWORD'],
)
with conn.cursor() as cur:
    for t in ['employees','order_item_refunds','order_items','orders',
              'products','users','website_pageviews','website_sessions']:
        cur.execute(f'SELECT COUNT(*) FROM \"{t}\"')
        print(f'{t}: {cur.fetchone()[0]}')
conn.close()
"
```

Expected: all 8 tables present with non-zero row counts matching the MySQL source.

- [ ] **Step 4: Commit**

```bash
git add pipeline.py
git commit -m "feat: call load_all_raw_tables from main() to complete raw copy step"
```
