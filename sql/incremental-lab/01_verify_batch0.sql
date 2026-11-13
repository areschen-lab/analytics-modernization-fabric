
SELECT
    assigned_batch,
    COUNT_BIG(*) AS order_count
FROM dbt_lab.stg_lab_orders
GROUP BY assigned_batch
ORDER BY assigned_batch;
