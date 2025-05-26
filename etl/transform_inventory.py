def transform_inventory(raw_inventory):
    transformed = []

    for item in raw_inventory:
        raw_time = item.get('lastUpdatedTime')
        clean_time = raw_time.replace("T", " ").replace("Z", "") if raw_time else None

        # Data safety and normalization
        asin = item.get('asin', 'UNKNOWN')
        asin = asin.strip().upper() if isinstance(asin, str) else 'UNKNOWN'

        sku = item.get('sellerSku', 'UNKNOWN')
        sku = sku.strip().upper() if isinstance(sku, str) else 'UNKNOWN'

        condition_type = item.get('condition')
        if isinstance(condition_type, str) and condition_type.strip():
            condition_type = condition_type.strip().upper()
        else:
            condition_type = 'UNKNOWN'

        fulfillment_center = item.get('fulfillmentCenterId')
        if isinstance(fulfillment_center, str):
            fulfillment_center = fulfillment_center.strip().upper()
        else:
            fulfillment_center = None

        try:
            quantity = int(item.get('totalQuantity', 0))
        except (TypeError, ValueError):
            quantity = 0

        country = item.get('country')
        if isinstance(country, str) and country.strip():
            country = country.strip().upper()
        else:
            country = 'N/A'

        transformed.append({
            'asin': asin,
            'sku': sku,
            'fulfillment_center': fulfillment_center,
            'condition_type': condition_type,
            'quantity': quantity,
            'last_updated': clean_time,
            'country': country
        })

    return transformed