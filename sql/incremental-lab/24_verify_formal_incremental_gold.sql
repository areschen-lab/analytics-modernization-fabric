
-- Stage 7.3: Formal Gold incremental reconciliation

WITH expected_orders AS (
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
    FROM dbt_dev.stg_orders o
    LEFT JOIN dbt_dev.int_payments_by_order p
        ON o.order_id = p.order_id
    LEFT JOIN dbt_dev.int_reviews_canonical r
        ON o.order_id = r.order_id
    LEFT JOIN dbt_dev.dim_customer c
        ON o.customer_id = c.customer_id
),

actual_orders AS (
    SELECT
        order_id, customer_id, customer_unique_id,
        order_status, order_purchase_timestamp,
        order_purchase_date, order_delivered_customer_date,
        order_estimated_delivery_date, delivery_days,
        delay_days, is_late, payment_total, payment_count,
        review_id, review_score, review_comment_title,
        review_comment_message, review_creation_date,
        review_answer_timestamp
    FROM dbt_dev.fact_orders
),

missing_orders AS (
    SELECT * FROM expected_orders
    EXCEPT
    SELECT * FROM actual_orders
),

extra_orders AS (
    SELECT * FROM actual_orders
    EXCEPT
    SELECT * FROM expected_orders
),

expected_items AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        CAST(price AS DECIMAL(18,2)) AS price,
        CAST(freight_value AS DECIMAL(18,2)) AS freight_value,
        CAST(price AS DECIMAL(18,2))
            + CAST(freight_value AS DECIMAL(18,2)) AS item_total
    FROM analytics_lakehouse.dbo.silver_order_items
),

actual_items AS (
    SELECT
        order_id, order_item_id, product_id, seller_id,
        price, freight_value, item_total
    FROM dbt_dev.fact_order_items
),

missing_items AS (
    SELECT * FROM expected_items
    EXCEPT
    SELECT * FROM actual_items
),

extra_items AS (
    SELECT * FROM actual_items
    EXCEPT
    SELECT * FROM expected_items
)

SELECT
    'fact_orders' AS fact_table,
    (SELECT COUNT_BIG(*) FROM expected_orders) AS expected_rows,
    (SELECT COUNT_BIG(*) FROM actual_orders) AS actual_rows,
    (SELECT COUNT_BIG(*) FROM missing_orders) AS missing_or_changed,
    (SELECT COUNT_BIG(*) FROM extra_orders) AS extra_or_changed

UNION ALL

SELECT
    'fact_order_items',
    (SELECT COUNT_BIG(*) FROM expected_items),
    (SELECT COUNT_BIG(*) FROM actual_items),
    (SELECT COUNT_BIG(*) FROM missing_items),
    (SELECT COUNT_BIG(*) FROM extra_items);
