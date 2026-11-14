
-- 1. All orders are now visible
SELECT COUNT_BIG(*) AS visible_orders
FROM dbt_lab.stg_lab_orders;

-- 2. Orders requiring incremental processing
SELECT
    COUNT_BIG(*) AS changed_orders,
    SUM(CASE WHEN o.assigned_batch = 2 THEN 1 ELSE 0 END)
        AS new_orders,
    SUM(CASE WHEN o.assigned_batch < 2 THEN 1 ELSE 0 END)
        AS historical_orders
FROM dbt_lab.int_lab_changed_order_ids ch
JOIN dbt_lab.stg_lab_orders o
    ON ch.order_id = o.order_id;

-- 3. Delayed events released in Batch 2
SELECT
    record_type,
    COUNT_BIG(*) AS delayed_events,
    COUNT_BIG(DISTINCT order_id) AS affected_orders
FROM dbt_lab.delayed_records
WHERE available_batch = 2
GROUP BY record_type;

-- 4. Fact should still contain Batch 1 data
SELECT COUNT_BIG(*) AS fact_orders
FROM dbt_lab.fact_lab_orders;
