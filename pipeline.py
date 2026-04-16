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
                    DATE_FORMAT(o.created_at, '%%Y-%%m-01') AS `year_month`,
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
