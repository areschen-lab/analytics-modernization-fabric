
{{ config(materialized='view') }}

SELECT
    order_id,
    SUM(
        CAST(payment_value AS DECIMAL(18,2))
    ) AS payment_total,
    COUNT(*) AS payment_count
FROM {{ ref('stg_lab_payments') }}
GROUP BY order_id
