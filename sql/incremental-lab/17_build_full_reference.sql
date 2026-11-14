
-- Full reconstruction directly from Silver data
-- Independent of batch_control and dbt staging views

CREATE TABLE dbt_lab.fact_lab_orders_full_reference AS
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

FROM analytics_lakehouse.dbo.silver_orders o

LEFT JOIN (
    SELECT
        order_id,
        SUM(payment_value) AS payment_total,
        COUNT(*) AS payment_count
    FROM analytics_lakehouse.dbo.silver_order_payments
    GROUP BY order_id
) p
    ON o.order_id = p.order_id

LEFT JOIN (
    SELECT
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY
                    CASE WHEN review_answer_timestamp IS NULL
                         THEN 1 ELSE 0 END,
                    review_answer_timestamp ASC,
                    CASE WHEN review_creation_date IS NULL
                         THEN 1 ELSE 0 END,
                    review_creation_date ASC,
                    review_id ASC
            ) AS rn
        FROM analytics_lakehouse.dbo.silver_order_reviews
    ) ranked
    WHERE rn = 1
) r
    ON o.order_id = r.order_id;


-- Validate reference
SELECT
    COUNT_BIG(*) AS total_orders,
    COUNT_BIG(DISTINCT order_id) AS unique_orders,
    SUM(payment_total) AS total_payment
FROM dbt_lab.fact_lab_orders_full_reference;
