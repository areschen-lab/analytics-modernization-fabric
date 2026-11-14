
-- Verify Batch 1 staging orders
SELECT COUNT_BIG(*) AS visible_orders
FROM dbt_lab.stg_lab_orders;

-- Verify changed order IDs
SELECT COUNT_BIG(*) AS changed_orders
FROM dbt_lab.int_lab_changed_order_ids;

-- Verify Fact has NOT been updated yet
SELECT COUNT_BIG(*) AS fact_orders
FROM dbt_lab.fact_lab_orders;
