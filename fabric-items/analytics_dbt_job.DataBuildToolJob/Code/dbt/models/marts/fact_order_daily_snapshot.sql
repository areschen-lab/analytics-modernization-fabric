
{{
    config(
        materialized='incremental',
        unique_key=['snapshot_date', 'order_id'],
        incremental_strategy='merge',
        full_refresh=false
    )
}}

SELECT
    c.snapshot_date,
    o.order_id,
    o.customer_id,
    o.customer_unique_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    o.delivery_days,
    o.delay_days,
    o.is_late,
    o.payment_total,
    o.payment_count,
    o.review_id,
    o.review_score

FROM {{ ref('fact_orders') }} o

CROSS JOIN {{ source('dbo', 'silver_batch_context') }} c

WHERE c.snapshot_date IS NOT NULL
