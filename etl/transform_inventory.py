from datetime import datetime

def transform_inventory(raw_inventory):
    transformed = []

    for item in raw_inventory:
        if not item.get('asin') or not item.get('sellerSku'):
            continue  # Skip items missing critical identifiers

        raw_time = item.get('lastUpdatedTime')
        clean_time = raw_time.replace("T", " ").replace("Z", "") if raw_time else None

        transformed.append({
            'asin': item.get('asin'),
            'sku': item.get('sellerSku'),
            'fulfillment_center': item.get('inventoryDetails', {}).get('fulfillmentCenterId'),
            'condition_type': item.get('condition'),
            'quantity': int(item.get('totalQuantity', 0)),
            'last_updated': clean_time
        })

    return transformed