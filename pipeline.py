import os

import pymysql
import pymysql.cursors
from dotenv import load_dotenv

from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP

import psycopg2
import psycopg2.extras
from datetime import datetime, timezone

load_dotenv()


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
                    DATE_FORMAT(o.created_at, '%Y-%m-01') AS `year_month`,
                    p.product_id,
                    p.product_name,
                    oi.price_usd                            AS item_revenue
                FROM orders o
                JOIN order_items oi ON o.order_id = oi.order_id
                JOIN products    p  ON oi.product_id = p.product_id
                ORDER BY `year_month`, p.product_id
            """)
            return cursor.fetchall()
    finally:
        conn.close()


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
        'revenue': Decimal('0'),
    })

    for row in rows:
        key = (row['year_month'], row['product_id'])
        # product_name is consistent within a product_id; last-write is fine
        groups[key]['product_name'] = row['product_name']
        groups[key]['order_ids'].add(row['order_id'])
        groups[key]['revenue'] += Decimal(str(row['item_revenue']))

    result = []
    for (year_month, product_id), data in sorted(groups.items()):
        order_count = len(data['order_ids'])
        revenue = float(data['revenue'].quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
        result.append({
            'year_month': year_month,
            'product_id': product_id,
            'product_name': data['product_name'],
            'order_count': order_count,
            'revenue_usd': revenue,
            'avg_order_value_usd': round(revenue / order_count, 2),
        })
    return result


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
    if not rows:
        raise ValueError("Load produced empty rows list — aborting to protect destination")

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
                    loaded_at             TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
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
                    (%(year_month)s::DATE, %(product_id)s, %(product_name)s,
                     %(order_count)s, %(revenue_usd)s, %(avg_order_value_usd)s,
                     %(loaded_at)s)
            """, [{**row, 'loaded_at': loaded_at} for row in rows])
            cursor.execute("SELECT COUNT(*) FROM monthly_sales")
            count = cursor.fetchone()[0]
            if count == 0:
                raise RuntimeError("INSERT succeeded but COUNT(*) returned 0 — aborting")
        conn.commit()
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def extract_and_load_raw_table(mysql_conn, pg_conn, table_name):
    """
    Copy one MySQL table into PostgreSQL as-is.
    Drops and recreates the table to reflect current MySQL schema, then bulk-inserts all rows.
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

    try:
        with pg_conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            cur.execute(f'CREATE TABLE "{table_name}" ({col_defs})')
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
    except Exception:
        pg_conn.rollback()
        raise
    return len(rows)


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
