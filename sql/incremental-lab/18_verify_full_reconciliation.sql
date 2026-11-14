
-- 1. Compare high-level totals

SELECT
    'Full Reference' AS source_name,
    COUNT_BIG(*) AS order_count,
    COUNT_BIG(DISTINCT order_id) AS unique_orders,
    SUM(payment_total) AS total_payment
FROM dbt_lab.fact_lab_orders_full_reference

UNION ALL

SELECT
    'Incremental Fact',
    COUNT_BIG(*),
    COUNT_BIG(DISTINCT order_id),
    SUM(payment_total)
FROM dbt_lab.fact_lab_orders;


-- 2. Compare every order and every business column
-- Normalize only monetary values to cents for comparison

WITH expected AS (
    SELECT
        order_id, customer_id, order_status,
        order_purchase_timestamp, order_purchase_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        delivery_days, delay_days, is_late,
        CAST(ROUND(payment_total, 2) AS DECIMAL(18,2))
            AS payment_total,
        payment_count, review_id, review_score,
        review_comment_title, review_comment_message,
        review_creation_date, review_answer_timestamp
    FROM dbt_lab.fact_lab_orders_full_reference
),
actual AS (
    SELECT
        order_id, customer_id, order_status,
        order_purchase_timestamp, order_purchase_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        delivery_days, delay_days, is_late,
        CAST(ROUND(payment_total, 2) AS DECIMAL(18,2))
            AS payment_total,
        payment_count, review_id, review_score,
        review_comment_title, review_comment_message,
        review_creation_date, review_answer_timestamp
    FROM dbt_lab.fact_lab_orders
),
missing_or_changed AS (
    SELECT * FROM expected
    EXCEPT
    SELECT * FROM actual
),
extra_or_changed AS (
    SELECT * FROM actual
    EXCEPT
    SELECT * FROM expected
)
SELECT
    (SELECT COUNT_BIG(*) FROM missing_or_changed)
        AS missing_or_changed_rows,
    (SELECT COUNT_BIG(*) FROM extra_or_changed)
        AS extra_or_changed_rows;
