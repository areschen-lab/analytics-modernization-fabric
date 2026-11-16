
SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    CAST(price AS DECIMAL(18,2)) AS price,
    CAST(freight_value AS DECIMAL(18,2)) AS freight_value,
    CAST(price AS DECIMAL(18,2))
        + CAST(freight_value AS DECIMAL(18,2)) AS item_total
FROM {{ source('dbo', 'silver_order_items') }}