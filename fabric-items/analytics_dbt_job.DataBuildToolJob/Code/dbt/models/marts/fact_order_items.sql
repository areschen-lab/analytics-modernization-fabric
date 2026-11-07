
SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value,
    item_total
FROM {{ source('dbo', 'silver_order_items') }}