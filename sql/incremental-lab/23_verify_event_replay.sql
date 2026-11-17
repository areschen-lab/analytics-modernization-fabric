WITH expected AS (
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
    FROM dbt_lab.stg_lab_orders o
    LEFT JOIN dbt_lab.dim_customer c
        ON o.customer_id = c.customer_id
    LEFT JOIN dbt_lab.int_lab_payments_by_order p
        ON o.order_id = p.order_id
    LEFT JOIN dbt_lab.int_lab_reviews_canonical r
        ON o.order_id = r.order_id
),
actual AS (
    SELECT
        order_id, customer_id, customer_unique_id,
        order_status, order_purchase_timestamp,
        order_purchase_date, order_delivered_customer_date,
        order_estimated_delivery_date, delivery_days,
        delay_days, is_late, payment_total, payment_count,
        review_id, review_score, review_comment_title,
        review_comment_message, review_creation_date,
        review_answer_timestamp
    FROM dbt_lab.fact_lab_orders
),
missing_rows AS (
    SELECT * FROM expected
    EXCEPT
    SELECT * FROM actual
),
extra_rows AS (
    SELECT * FROM actual
    EXCEPT
    SELECT * FROM expected
)
SELECT
    'Orders' AS fact_table,
    (SELECT COUNT_BIG(*) FROM expected) AS expected_rows,
    (SELECT COUNT_BIG(*) FROM actual) AS actual_rows,
    (SELECT COUNT_BIG(*) FROM missing_rows) AS missing_or_changed,
    (SELECT COUNT_BIG(*) FROM extra_rows) AS extra_or_changed;


WITH expected AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        CAST(price AS DECIMAL(18,2)) AS price,
        CAST(freight_value AS DECIMAL(18,2)) AS freight_value,
        CAST(price AS DECIMAL(18,2))
            + CAST(freight_value AS DECIMAL(18,2)) AS item_total
    FROM dbt_lab.stg_lab_order_items
),
actual AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        price,
        freight_value,
        item_total
    FROM dbt_lab.fact_lab_order_items
),
missing_rows AS (
    SELECT * FROM expected
    EXCEPT
    SELECT * FROM actual
),
extra_rows AS (
    SELECT * FROM actual
    EXCEPT
    SELECT * FROM expected
)
SELECT
    'Order Items' AS fact_table,
    (SELECT COUNT_BIG(*) FROM expected) AS expected_rows,
    (SELECT COUNT_BIG(*) FROM actual) AS actual_rows,
    (SELECT COUNT_BIG(*) FROM missing_rows) AS missing_or_changed,
    (SELECT COUNT_BIG(*) FROM extra_rows) AS extra_or_changed;