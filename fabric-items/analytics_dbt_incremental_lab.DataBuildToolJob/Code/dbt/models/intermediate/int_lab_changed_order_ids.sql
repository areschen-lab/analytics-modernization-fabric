
{{ config(materialized='view') }}

WITH new_orders AS (
    -- Orders first appearing in the current batch
    SELECT o.order_id
    FROM {{ ref('stg_lab_orders') }} o
    CROSS JOIN dbt_lab.batch_control c
    WHERE c.control_id = 1
      AND o.assigned_batch = c.current_batch
),

late_updates AS (
    -- Existing orders receiving delayed payments or reviews
    SELECT d.order_id
    FROM dbt_lab.delayed_records d
    INNER JOIN dbt_lab.batch_control c
        ON c.control_id = 1
       AND d.available_batch = c.current_batch
    INNER JOIN {{ ref('stg_lab_orders') }} o
        ON o.order_id = d.order_id
)

-- UNION deduplicates orders affected by multiple events
SELECT order_id FROM new_orders

UNION

SELECT order_id FROM late_updates
