from collections import defaultdict


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
        'revenue': 0.0,
    })

    for row in rows:
        key = (row['year_month'], row['product_id'])
        groups[key]['product_name'] = row['product_name']
        groups[key]['order_ids'].add(row['order_id'])
        groups[key]['revenue'] += float(row['item_revenue'])

    result = []
    for (year_month, product_id), data in sorted(groups.items()):
        order_count = len(data['order_ids'])
        revenue = round(data['revenue'], 2)
        result.append({
            'year_month': year_month,
            'product_id': product_id,
            'product_name': data['product_name'],
            'order_count': order_count,
            'revenue_usd': revenue,
            'avg_order_value_usd': round(revenue / order_count, 2),
        })
    return result
