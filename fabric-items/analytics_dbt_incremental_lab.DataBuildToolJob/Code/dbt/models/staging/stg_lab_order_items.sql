{{ config(materialized='view') }}

SELECT i.*
FROM {{ source('dbo', 'silver_order_items') }} i

INNER JOIN {{ ref('stg_lab_orders') }} o
    ON i.order_id = o.order_id