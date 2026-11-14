
SELECT
    order_id,
    payment_total,
    payment_count
FROM dbt_lab.int_lab_payments_by_order
WHERE order_id = '0016dfedd97fc2950e388d2971d718c7';
