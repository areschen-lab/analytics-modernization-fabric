
{{ config(materialized='view') }}

SELECT
    r.*
FROM {{ source('dbo', 'silver_order_reviews') }} r

INNER JOIN {{ ref('stg_lab_orders') }} o
    ON r.order_id = o.order_id

CROSS JOIN dbt_lab.batch_control c

LEFT JOIN dbt_lab.delayed_records d
    ON d.record_type = 'review'
    AND d.order_id = r.order_id
    AND d.review_id = r.review_id

WHERE c.control_id = 1
  AND (
      d.order_id IS NULL
      OR d.available_batch <= c.current_batch
  )
