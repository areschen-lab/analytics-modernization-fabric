{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'order_item_id'],
        incremental_strategy='merge'
    )
}}

WITH current_state AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        CAST(price AS DECIMAL(18,2)) AS price,
        CAST(freight_value AS DECIMAL(18,2)) AS freight_value,
        CAST(price AS DECIMAL(18,2))
            + CAST(freight_value AS DECIMAL(18,2)) AS item_total
    FROM {{ ref('stg_lab_order_items') }}
)

SELECT *
FROM current_state

{% if is_incremental() %}

EXCEPT

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value,
    item_total
FROM {{ this }}

{% endif %}