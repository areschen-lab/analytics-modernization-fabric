{{ config(materialized='view') }}

SELECT
    p.*
FROM {{ source('dbo', 'silver_order_payments') }} p

-- Only payments belonging to visible orders
INNER JOIN {{ ref('stg_lab_orders') }} o
    ON p.order_id = o.order_id

CROSS JOIN dbt_lab.batch_control c

-- Check whether this payment is intentionally delayed
LEFT JOIN dbt_lab.delayed_records d
    ON d.record_type = 'payment'
    AND d.order_id = p.order_id
    AND d.payment_sequential = p.payment_sequential

WHERE c.control_id = 1
  AND (
      d.order_id IS NULL
      OR d.available_batch <= c.current_batch
  )
