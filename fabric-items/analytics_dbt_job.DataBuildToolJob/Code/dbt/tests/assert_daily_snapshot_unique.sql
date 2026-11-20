SELECT
    snapshot_date,
    order_id,
    COUNT(*) AS row_count
FROM {{ ref('fact_order_daily_snapshot') }}
GROUP BY snapshot_date, order_id
HAVING COUNT(*) > 1