import pytest
from pipeline import extract


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
        assert row['year_month'].endswith('-01'), \
            f"Expected year_month to end in '-01', got: {row['year_month']}"


def test_extract_known_products():
    rows = extract()
    product_names = {row['product_name'] for row in rows}
    assert 'The Original Gift Basket' in product_names
