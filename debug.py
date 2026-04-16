import os
import pymysql
import pymysql.cursors
from dotenv import load_dotenv

load_dotenv()

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
                DATE_FORMAT(o.created_at, '%Y-%m-01') AS `year_month`,
                o.created_at
            FROM orders o
            LIMIT 5
        """)
        rows = cursor.fetchall()
        for row in rows:
            print(row)
finally:
    conn.close()
