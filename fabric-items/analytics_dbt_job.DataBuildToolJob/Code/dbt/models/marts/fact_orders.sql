
SELECT
    o.order_id,
    o.customer_id,
    c.customer_unique_id,

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

FROM {{ ref('stg_orders') }} AS o

LEFT JOIN {{ ref('int_payments_by_order') }} AS p
    ON o.order_id = p.order_id

LEFT JOIN {{ ref('int_reviews_canonical') }} AS r
    ON o.order_id = r.order_id

LEFT JOIN {{ ref('dim_customer') }} AS c
    ON o.customer_id = c.customer_id
