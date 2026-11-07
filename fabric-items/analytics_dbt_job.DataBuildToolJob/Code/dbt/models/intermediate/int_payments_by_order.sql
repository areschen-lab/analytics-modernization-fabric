SELECT
    order_id,
    SUM(payment_value) AS payment_total,
    COUNT(*) AS payment_count
FROM {{ source('dbo', 'silver_order_payments') }}
GROUP BY order_id