
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

-- depends_on: {{ ref('int_lab_changed_order_ids') }}

SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    CAST(o.order_purchase_timestamp AS DATE) AS order_purchase_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    o.delivery_days,
    o.delay_days,
    o.is_late,

    p.payment_total,
    p.payment_count,

    r.review_id,
    r.review_score,
    r.review_comment_title,
    r.review_comment_message,
    r.review_creation_date,
    r.review_answer_timestamp

FROM {{ ref('stg_lab_orders') }} o

LEFT JOIN {{ ref('int_lab_payments_by_order') }} p
    ON o.order_id = p.order_id

LEFT JOIN {{ ref('int_lab_reviews_canonical') }} r
    ON o.order_id = r.order_id

{% if is_incremental() %}

INNER JOIN {{ ref('int_lab_changed_order_ids') }} ch
    ON o.order_id = ch.order_id

{% endif %}
