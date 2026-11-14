
-- 1. Check payment_total data type
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dbt_lab'
  AND TABLE_NAME = 'int_lab_payments_by_order'
  AND COLUMN_NAME = 'payment_total';

-- 2. Check total payment
SELECT
    COUNT_BIG(*) AS orders_with_payments,
    SUM(payment_total) AS total_payment
FROM dbt_lab.int_lab_payments_by_order;
