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
    # order_id 1 has two line items for the same product — one order, revenue summed
    rows = [
        {'order_id': 1, 'year_month': '2024-01-01', 'product_id': 1,
         'product_name': 'The Original Gift Basket', 'item_revenue': 49.99},
        {'order_id': 1, 'year_month': '2024-01-01', 'product_id': 1,
         'product_name': 'The Original Gift Basket', 'item_revenue': 25.00},
    ]
    result = transform(rows)
    assert result[0]['order_count'] == 1
    assert result[0]['revenue_usd'] == 74.99   # both line items summed
    assert result[0]['avg_order_value_usd'] == 74.99  # revenue / 1 order


def test_transform_empty_input():
    assert transform([]) == []


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
