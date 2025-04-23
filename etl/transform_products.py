from datetime import datetime

def transform_products(raw_products):
    transformed = []

    for product in raw_products:
        try:
            raw_time = product.get('last_updated')
            clean_time = datetime.strptime(raw_time.replace("T", " ").replace("Z", ""), "%Y-%m-%d %H:%M:%S") if raw_time else None
        except ValueError:
            clean_time = None

        transformed.append({
            'asin': product.get('asin'),
            'sku': product.get('sku'),
            'product_title': product.get('product_title'),
            'price': float(product.get('price', 0)),
            'currency_code': product.get('currency_code', 'USD'),
            'stock_quantity': int(product.get('stock_quantity', 0)),
            'last_updated': clean_time
        })

    return transformed