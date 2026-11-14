-- 1. 确认当前批次
SELECT *
FROM dbt_lab.batch_control;

-- 2. 检查 Fact 行数、唯一性和支付总额
SELECT
    COUNT_BIG(*) AS total_orders,
    COUNT_BIG(DISTINCT order_id) AS unique_orders,
    SUM(payment_total) AS total_payment
FROM dbt_lab.fact_lab_orders;

-- 3. 检查延迟支付的测试订单
SELECT
    order_id,
    payment_total,
    payment_count
FROM dbt_lab.fact_lab_orders
WHERE order_id = '0016dfedd97fc2950e388d2971d718c7';