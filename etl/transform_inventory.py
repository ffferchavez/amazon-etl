from datetime import datetime

def transform_inventory(raw_inventory):
    transformed = []

    for item in raw_inventory:
        raw_time = item.get('last_updated')
        clean_time = None
        if raw_time:
            try:
                clean_time = datetime.strptime(raw_time.replace("T", " ").replace("Z", ""), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                clean_time = None

        transformed.append({
            'asin': item.get('asin'),
            'sku': item.get('sku'),
            'fulfillment_center': item.get('fulfillment_center'),
            'condition_type': item.get('condition_type'),
            'quantity': int(item.get('quantity', 0)),
            'last_updated': clean_time
        })

    return transformed