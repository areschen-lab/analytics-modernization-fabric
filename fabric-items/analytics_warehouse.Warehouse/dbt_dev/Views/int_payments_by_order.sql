create view [dbt_dev].[int_payments_by_order] as SELECT
    order_id,
    SUM(CAST(payment_value AS DECIMAL(18,2))) AS payment_total,
    COUNT(*) AS payment_count
FROM analytics_lakehouse.dbo.silver_order_payments
GROUP BY order_id;

GO