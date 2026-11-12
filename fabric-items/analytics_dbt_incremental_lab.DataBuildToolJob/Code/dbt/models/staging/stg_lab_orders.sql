
{{ config(materialized='view') }}

WITH ranked_orders AS (
    SELECT
        o.*,
        NTILE(20) OVER (
            ORDER BY o.order_purchase_timestamp, o.order_id
        ) AS batch_group
    FROM {{ source('dbo', 'silver_orders') }} o
),

assigned_orders AS (
    SELECT
        *,
        CASE
            WHEN batch_group <= 18 THEN 0
            WHEN batch_group = 19 THEN 1
            ELSE 2
        END AS assigned_batch
    FROM ranked_orders
)

SELECT o.*
FROM assigned_orders o
CROSS JOIN dbt_lab.batch_control c
WHERE c.control_id = 1
  AND o.assigned_batch <= c.current_batch
