def transform_products(raw_products):
    transformed = []

    for product in raw_products:
        # real SP API product data structure:
        # {
        #     'asin': product['Identifiers']['MarketplaceASIN']['ASIN'],
        #     'sku': product['SellerSKU'],
        #     'product_title': product['AttributeSets'][0]['Title'],
        #     'price': float(product['Offers'][0]['BuyingPrice']['ListingPrice']['Amount']),
        #     'currency_code': product['Offers'][0]['BuyingPrice']['ListingPrice']['CurrencyCode'],
        #     'stock_quantity': int(product['Offers'][0]['Quantity']),
        #     'last_updated': product['Summary']['LastUpdateDate']
        # }
        raw_time = product.get('last_updated')
        clean_time = raw_time.replace("T", " ").replace("Z", "") if raw_time else None

        transformed.append({
            'asin': product['asin'],
            'sku': product['sku'],
            'product_title': product['product_title'],
            'price': float(product['price']),
            'currency_code': product.get('currency_code', 'USD'),
            'stock_quantity': int(product.get('stock_quantity', 0)),
            'last_updated': clean_time
        })

    return transformed