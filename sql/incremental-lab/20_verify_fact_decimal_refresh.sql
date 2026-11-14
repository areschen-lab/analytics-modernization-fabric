
-- Verify current batch
SELECT current_batch
FROM dbt_lab.batch_control
WHERE control_id = 1;


-- 1. Verify Fact payment_total data type
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dbt_lab'
  AND TABLE_NAME = 'fact_lab_orders'
  AND COLUMN_NAME = 'payment_total';


-- 2. Verify final Fact
SELECT
    COUNT_BIG(*) AS total_orders,
    COUNT_BIG(DISTINCT order_id) AS unique_orders,
    SUM(payment_total) AS total_payment
FROM dbt_lab.fact_lab_orders;


-- 3. Verify known delayed payment
SELECT
    order_id,
    payment_total,
    payment_count
FROM dbt_lab.fact_lab_orders
WHERE order_id = '0016dfedd97fc2950e388d2971d718c7';
