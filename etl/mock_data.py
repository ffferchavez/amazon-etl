# etl/mock_data.py

mock_orders = [
    {
        "AmazonOrderId": "123-000001-000001",
        "OrderStatus": "Shipped",
        "PurchaseDate": "2024-03-01T12:00:00Z",
        "BuyerEmail": "buyer1@marketplace.amazon.com",
        "OrderTotal": {
            "CurrencyCode": "USD",
            "Amount": "25.50"
        }
    },
    {
        "AmazonOrderId": "123-000002-000002",
        "OrderStatus": "Unshipped",
        "PurchaseDate": "2024-03-02T16:45:00Z",
        "BuyerEmail": "buyer2@marketplace.amazon.com",
        "OrderTotal": {
            "CurrencyCode": "USD",
            "Amount": "130.99"
        }
    },
    {
        "AmazonOrderId": "123-000003-000003",
        "OrderStatus": "Canceled",
        "PurchaseDate": "2024-03-03T09:30:00Z",
        "BuyerEmail": "buyer3@marketplace.amazon.com",
        "OrderTotal": {
            "CurrencyCode": "EUR",
            "Amount": "49.00"
        }
    }
]

mock_products = [
    {
        "asin": "B08XYZ1234",
        "sku": "SKU-001",
        "product_title": "Wireless Mouse",
        "price": 19.99,
        "currency_code": "USD",
        "stock_quantity": 85,
        "last_updated": "2024-03-24T12:00:00Z"
    },
    {
        "asin": "B09XYZ5678",
        "sku": "SKU-002",
        "product_title": "Mechanical Keyboard",
        "price": 59.99,
        "currency_code": "USD",
        "stock_quantity": 40,
        "last_updated": "2024-03-24T12:00:00Z"
    }
]

mock_inventory = [
    {
        "asin": "B08XYZ1234",
        "sku": "SKU-001",
        "fulfillment_center": "AMAZON_FC_US1",
        "condition_type": "NewItem",
        "quantity": 25,
        "last_updated": "2024-03-24T12:00:00Z"
    },
    {
        "asin": "B09XYZ5678",
        "sku": "SKU-002",
        "fulfillment_center": "AMAZON_FC_US2",
        "condition_type": "NewItem",
        "quantity": 10,
        "last_updated": "2024-03-24T12:00:00Z"
    }
]