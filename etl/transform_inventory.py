def transform_inventory(raw_inventory):
    transformed = []

    for item in raw_inventory:
        raw_time = item.get('last_updated')
        clean_time = raw_time.replace("T", " ").replace("Z", "") if raw_time else None

        transformed.append({
            'asin': item['asin'],
            'sku': item['sku'],
            'fulfillment_center': item.get('fulfillment_center'),
            'condition_type': item.get('condition_type'),
            'quantity': int(item.get('quantity', 0)),
            'last_updated': clean_time
        })

    return transformed