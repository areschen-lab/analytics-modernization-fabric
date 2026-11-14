
SELECT
    COUNT_BIG(*) AS changed_orders,
    COUNT_BIG(DISTINCT order_id) AS distinct_orders
FROM dbt_lab.int_lab_changed_order_ids;
