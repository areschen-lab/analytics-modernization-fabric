
-- Save the Fact before rerunning Batch 2
CREATE TABLE dbt_lab.fact_lab_orders_batch2_baseline AS
SELECT *
FROM dbt_lab.fact_lab_orders;

-- Verify baseline
SELECT
    COUNT_BIG(*) AS total_orders,
    COUNT_BIG(DISTINCT order_id) AS unique_orders,
    SUM(payment_total) AS total_payment
FROM dbt_lab.fact_lab_orders_batch2_baseline;
