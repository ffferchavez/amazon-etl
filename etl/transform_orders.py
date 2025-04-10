def transform_orders(raw_orders):
    transformed = []

    for order in raw_orders:
        # real SP API order data structure:
        # {
        #     'amazon_order_id': order['AmazonOrderId'],
        #     'order_status': order['OrderStatus'],
        #     'purchase_date': order['PurchaseDate'],
        #     'buyer_email': order.get('BuyerEmail'),
        #     'order_total': float(order['OrderTotal']['Amount']),
        #     'currency_code': order['OrderTotal']['CurrencyCode']
        # }
        transformed.append({
            'amazon_order_id': order['AmazonOrderId'],
            'order_status': order['OrderStatus'],
            'purchase_date': order['PurchaseDate'],
            'buyer_email': order.get('BuyerEmail', 'unknown@example.com'),
            'order_total': float(order.get('OrderTotal', {}).get('Amount', 0.0)),
            'currency_code': order.get('OrderTotal', {}).get('CurrencyCode', 'USD')
        })

    return transformed