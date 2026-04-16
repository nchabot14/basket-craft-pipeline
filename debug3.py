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
        # Test with single % (what should be sent)
        cursor.execute("SELECT DATE_FORMAT(NOW(), '%Y-%m-01') AS result")
        print("Single %:", cursor.fetchone())
        
        # Test with %% (which becomes % in the string)
        cursor.execute("SELECT DATE_FORMAT(NOW(), '%%Y-%%m-01') AS result")
        print("Double %%:", cursor.fetchone())
finally:
    conn.close()
