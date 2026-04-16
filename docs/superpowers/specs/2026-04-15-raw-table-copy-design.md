# Raw Table Copy — Design Spec

**Date:** 2026-04-15
**Author:** Nick Chabot
**Status:** Approved

---

## Overview

Extend `pipeline.py` to copy all 8 raw tables from the Basket Craft MySQL source database into the AWS RDS PostgreSQL instance as-is, with no transformations. Tables are truncated and fully reloaded on each run. Credentials are read from `.env`.

---

## Source Tables

**Database:** `basket_craft` on `db.isba.co:3306`

| Table | Rows |
|---|---|
| `products` | 4 |
| `employees` | 20 |
| `order_item_refunds` | 1,731 |
| `orders` | 32,313 |
| `order_items` | 40,025 |
| `users` | 31,696 |
| `website_sessions` | 472,871 |
| `website_pageviews` | 1,188,124 |

> Note: `users` contains PII (names, emails, addresses) and password salt/hash columns. Included by design.

---

## Destination

**Database:** `basket_craft` on AWS RDS PostgreSQL  
**Host:** `basket-craft-db.cv0ky2iimq0g.us-east-2.rds.amazonaws.com:5432`  
**Credentials:** `POSTGRES_*` keys in `.env`

Tables are created as plain heap tables — no primary keys or indexes. Raw staging tables are read targets only.

---

## Architecture

Three additions to `pipeline.py`:

### `_mysql_to_pg_type(mysql_type) -> str`

Maps a MySQL column type string to its PostgreSQL equivalent.

| MySQL | PostgreSQL |
|---|---|
| `int`, `int unsigned` | `INTEGER` |
| `smallint unsigned` | `SMALLINT` |
| `varchar(N)` | `VARCHAR(N)` |
| `decimal(m,n)` | `NUMERIC(m,n)` |
| `timestamp` | `TIMESTAMP` |
| `text` | `TEXT` |
| anything else | `TEXT` (fallback, with warning) |

### `extract_and_load_raw_table(mysql_conn, pg_conn, table_name) -> int`

Per-table workhorse:

```
1. DESCRIBE <table>        → column names + MySQL types
2. SELECT * FROM <table>   → all rows as dicts (DictCursor)
3. Build PostgreSQL DDL from column list
4. CREATE TABLE IF NOT EXISTS <table> (<col_defs>)
5. TRUNCATE <table>
6. execute_batch INSERT     → 1,000 rows/batch
7. COMMIT
8. Return row count
```

Each table commits independently. If one table fails, already-committed tables remain intact.

### `load_all_raw_tables()`

Orchestrator:

```
1. Open MySQL connection  (MYSQL_* from .env)
2. Open PostgreSQL connection  (POSTGRES_* from .env)
3. For each table in TABLES_TO_COPY:
       extract_and_load_raw_table(...)
       print("  <table>: N rows")
4. Close both connections
```

`TABLES_TO_COPY` is a module-level constant listing all 8 tables.

### `main()` update

Gains a new section after the existing ETL:

```
print("Loading raw tables into PostgreSQL...")
load_all_raw_tables()
print("Done.")
```

---

## Data Flow

```
MySQL (basket_craft)
  DESCRIBE <table>  ──→  column schema
  SELECT *          ──→  rows (DictCursor dicts)
        │
        ▼
  _mysql_to_pg_type()  →  PostgreSQL DDL
  CREATE TABLE IF NOT EXISTS
  TRUNCATE
  execute_batch INSERT (1,000 rows/batch)
  COMMIT
        │
        ▼
PostgreSQL (basket_craft on RDS)
  raw staging tables (heap, no indexes)
```

---

## Error Handling

| Failure | Behavior |
|---|---|
| MySQL connection fails | Raises immediately — PostgreSQL untouched |
| `DESCRIBE` / `SELECT *` fails | Exception propagates — that table's transaction never opens |
| PostgreSQL insert fails mid-table | Rolls back that table — other committed tables unaffected |
| Unknown MySQL type | Falls back to `TEXT` with a `print(f"Warning: unknown type...")` |

---

## Memory Profile

`fetchall()` loads each table fully into memory before inserting. Estimated peak:

| Table | Est. size in memory |
|---|---|
| `website_pageviews` | ~120 MB |
| `website_sessions` | ~60 MB |
| All others | < 10 MB each |

Acceptable for a class/dev environment. A production version would stream rows in chunks rather than using `fetchall()`.

---

## Libraries

No new dependencies. Uses existing `pymysql`, `psycopg2`, `psycopg2.extras`, and `python-dotenv`.

---

## Amendments (post-implementation)

Two deviations from the original spec were made during implementation:

1. **`DROP TABLE IF EXISTS` + `CREATE TABLE`** replaces `CREATE TABLE IF NOT EXISTS` + `TRUNCATE`. Rebuilding the schema on every run self-heals if a column is added, renamed, or retyped in MySQL — `IF NOT EXISTS` would freeze the destination schema at first run and silently diverge.

2. **`try/except` with `pg_conn.rollback()`** wraps the PostgreSQL block in `extract_and_load_raw_table`. Without it, a mid-table failure would leave the shared psycopg2 connection in an aborted-transaction state, breaking every subsequent table in the orchestrator's loop.

Both changes strengthen the truncate-and-reload contract; neither alters the public function signatures.
