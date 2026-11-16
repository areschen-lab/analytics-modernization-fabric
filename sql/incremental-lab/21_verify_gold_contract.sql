-- Stage 7.1: Gold Data Contract Reconciliation
-- Main Gold vs Incremental Lab
-- Expected: 99441 rows each, 0 differences in both directions
-- payment_total normalized to DECIMAL(18,2) for comparison

WITH main_data AS (
    SELECT
        order_id, customer_id, customer_unique_id,
        order_status, order_purchase_timestamp,
        order_purchase_date, order_delivered_customer_date,
        order_estimated_delivery_date,
        delivery_days, delay_days, is_late,
        CAST(ROUND(payment_total, 2) AS DECIMAL(18,2)) AS payment_total,
        payment_count, review_id, review_score,
        review_comment_title, review_comment_message,
        review_creation_date, review_answer_timestamp
    FROM dbt_dev.fact_orders
),
lab_data AS (
    SELECT
        order_id, customer_id, customer_unique_id,
        order_status, order_purchase_timestamp,
        order_purchase_date, order_delivered_customer_date,
        order_estimated_delivery_date,
        delivery_days, delay_days, is_late,
        CAST(payment_total AS DECIMAL(18,2)) AS payment_total,
        payment_count, review_id, review_score,
        review_comment_title, review_comment_message,
        review_creation_date, review_answer_timestamp
    FROM dbt_lab.fact_lab_orders
),
only_main AS (
    SELECT * FROM main_data
    EXCEPT
    SELECT * FROM lab_data
),
only_lab AS (
    SELECT * FROM lab_data
    EXCEPT
    SELECT * FROM main_data
)
SELECT
    (SELECT COUNT_BIG(*) FROM main_data) AS main_rows,
    (SELECT COUNT_BIG(*) FROM lab_data) AS lab_rows,
    (SELECT COUNT_BIG(*) FROM only_main) AS only_main,
    (SELECT COUNT_BIG(*) FROM only_lab) AS only_lab;