from datetime import datetime

def transform_orders(raw_orders):
    transformed = []

    for order in raw_orders:
        # Format the purchase date
        purchase_date = order.get('PurchaseDate')
        try:
            parsed_date = datetime.strptime(purchase_date.replace("T", " ").replace("Z", ""), "%Y-%m-%d %H:%M:%S") if purchase_date else None
        except ValueError:
            parsed_date = None

        transformed.append({
            'amazon_order_id': order.get('AmazonOrderId'),
            'order_status': order.get('OrderStatus'),
            'purchase_date': parsed_date,
            'buyer_email': order.get('BuyerEmail', 'unknown@example.com'),
            'order_total': float(order.get('OrderTotal', {}).get('Amount', 0.0)),
            'currency_code': order.get('OrderTotal', {}).get('CurrencyCode', 'USD')
        })

    return transformed