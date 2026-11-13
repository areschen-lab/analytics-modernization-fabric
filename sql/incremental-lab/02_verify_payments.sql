
SELECT
    payment_sequential,
    payment_type,
    payment_value
FROM dbt_lab.stg_lab_payments
WHERE order_id = '0016dfedd97fc2950e388d2971d718c7'
ORDER BY payment_sequential;
