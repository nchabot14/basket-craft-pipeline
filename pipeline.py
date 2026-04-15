import os

import pymysql
import pymysql.cursors
from dotenv import load_dotenv

from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP

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
                ORDER BY 2, p.product_id
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
