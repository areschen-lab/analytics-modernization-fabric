
-- Compare row counts and payment totals
SELECT
    (SELECT COUNT_BIG(*)
     FROM dbt_lab.fact_lab_orders_batch2_baseline)
        AS before_orders,

    (SELECT COUNT_BIG(*)
     FROM dbt_lab.fact_lab_orders)
        AS after_orders,

    (SELECT SUM(payment_total)
     FROM dbt_lab.fact_lab_orders_batch2_baseline)
        AS before_payment,

    (SELECT SUM(payment_total)
     FROM dbt_lab.fact_lab_orders)
        AS after_payment;


-- Compare every column of every row
WITH before_minus_after AS (
    SELECT *
    FROM dbt_lab.fact_lab_orders_batch2_baseline
    EXCEPT
    SELECT *
    FROM dbt_lab.fact_lab_orders
),
after_minus_before AS (
    SELECT *
    FROM dbt_lab.fact_lab_orders
    EXCEPT
    SELECT *
    FROM dbt_lab.fact_lab_orders_batch2_baseline
)
SELECT
    (SELECT COUNT_BIG(*) FROM before_minus_after)
        AS missing_or_changed_rows,
    (SELECT COUNT_BIG(*) FROM after_minus_before)
        AS added_or_changed_rows;
