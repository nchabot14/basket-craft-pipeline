import os
import pytest
import psycopg2
from pipeline import extract, load


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
        assert isinstance(row['year_month'], str), \
            f"Expected year_month to be str, got {type(row['year_month'])}"
        assert row['year_month'].endswith('-01'), \
            f"Expected year_month to end in '-01', got: {row['year_month']}"


def test_extract_known_products():
    rows = extract()
    product_names = {row['product_name'] for row in rows}
    assert 'The Original Gift Basket' in product_names


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
    assert count == 1
    conn = _pg_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT product_id FROM monthly_sales ORDER BY product_id")
        product_ids = [row[0] for row in cur.fetchall()]
    conn.close()
    assert product_ids == [2]


def test_load_raises_on_empty_rows():
    with pytest.raises(ValueError, match="empty"):
        load([])


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
    pg_conn = _pg_conn()
    try:
        count = extract_and_load_raw_table(mysql_conn, pg_conn, 'products')
        assert count == 4
        with pg_conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM "products"')
            assert cur.fetchone()[0] == 4
    finally:
        mysql_conn.close()
        pg_conn.close()
