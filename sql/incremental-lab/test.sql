WITH current_state AS (
    SELECT
        o.order_id,
        o.order_status,
        o.order_delivered_customer_date,
        o.delivery_days,
        o.delay_days,
        o.is_late,
        r.review_id,
        r.review_score
    FROM dbt_lab.stg_lab_orders o
    LEFT JOIN dbt_lab.int_lab_reviews_canonical r
        ON o.order_id = r.order_id
)
SELECT COUNT_BIG(*) AS changed_orders
FROM (
    SELECT * FROM current_state
    EXCEPT
    SELECT
        order_id,
        order_status,
        order_delivered_customer_date,
        delivery_days,
        delay_days,
        is_late,
        review_id,
        review_score
    FROM dbt_lab.fact_lab_orders
) d;