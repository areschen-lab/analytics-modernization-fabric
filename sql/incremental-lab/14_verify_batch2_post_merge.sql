
-- 1. Verify final Fact row count
SELECT
    COUNT_BIG(*) AS total_orders,
    COUNT_BIG(DISTINCT order_id) AS unique_orders,
    SUM(payment_total) AS total_payment
FROM dbt_lab.fact_lab_orders;


-- 2. Verify all Batch 2 changed orders reached Fact
SELECT
    COUNT_BIG(*) AS changed_orders,
    COUNT_BIG(f.order_id) AS found_in_fact,
    SUM(CASE WHEN o.assigned_batch = 2 THEN 1 ELSE 0 END)
        AS new_orders,
    SUM(CASE WHEN o.assigned_batch < 2 THEN 1 ELSE 0 END)
        AS historical_orders
FROM dbt_lab.int_lab_changed_order_ids ch
JOIN dbt_lab.stg_lab_orders o
    ON ch.order_id = o.order_id
LEFT JOIN dbt_lab.fact_lab_orders f
    ON ch.order_id = f.order_id;


-- 3. Reconcile all Batch 2 historical updates
WITH affected_orders AS (
    SELECT DISTINCT order_id
    FROM dbt_lab.delayed_records
    WHERE available_batch = 2
),
expected AS (
    SELECT
        a.order_id,
        p.payment_total,
        p.payment_count,
        r.review_id,
        r.review_score
    FROM affected_orders a
    LEFT JOIN dbt_lab.int_lab_payments_by_order p
        ON a.order_id = p.order_id
    LEFT JOIN dbt_lab.int_lab_reviews_canonical r
        ON a.order_id = r.order_id
),
actual AS (
    SELECT
        f.order_id,
        f.payment_total,
        f.payment_count,
        f.review_id,
        f.review_score
    FROM dbt_lab.fact_lab_orders f
    JOIN affected_orders a
        ON f.order_id = a.order_id
)
SELECT * FROM expected
EXCEPT
SELECT * FROM actual;
