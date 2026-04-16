import os
import pymysql
import pymysql.cursors
from dotenv import load_dotenv
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP

load_dotenv()

def extract():
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
                LIMIT 5
            """)
            return cursor.fetchall()
    finally:
        conn.close()

rows = extract()
for row in rows:
    print(row)
