SELECT
    order_id,
    SUM(CAST(payment_value AS DECIMAL(18,2))) AS payment_total,
    COUNT(*) AS payment_count
FROM {{ source('dbo', 'silver_order_payments') }}
GROUP BY order_id