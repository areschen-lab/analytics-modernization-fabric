create view [dbt_lab].[stg_lab_orders] as 

WITH replay_clock AS (
    SELECT
        DATEADD(
            DAY,
            current_batch + 1,
            CAST('2018-06-10' AS DATE)
        ) AS cutoff_date
    FROM dbt_lab.batch_control
    WHERE control_id = 1
)

SELECT
    o.order_id,
    o.customer_id,
    o.order_purchase_timestamp,
    o.order_estimated_delivery_date,

    -- Reconstruct order status using actual event timestamps
    CASE
        WHEN o.order_delivered_customer_date < c.cutoff_date
            THEN 'delivered'
        WHEN o.order_delivered_carrier_date < c.cutoff_date
            THEN 'shipped'
        WHEN o.order_approved_at < c.cutoff_date
            THEN 'approved'
        ELSE 'created'
    END AS order_status,

    -- Delivery information becomes visible after actual delivery
    CASE
        WHEN o.order_delivered_customer_date < c.cutoff_date
            THEN o.order_delivered_customer_date
        ELSE NULL
    END AS order_delivered_customer_date,

    CASE
        WHEN o.order_delivered_customer_date < c.cutoff_date
            THEN o.delivery_days
        ELSE NULL
    END AS delivery_days,

    CASE
        WHEN o.order_delivered_customer_date < c.cutoff_date
            THEN o.delay_days
        ELSE NULL
    END AS delay_days,

    CASE
        WHEN o.order_delivered_customer_date < c.cutoff_date
            THEN o.is_late
        ELSE NULL
    END AS is_late

FROM analytics_lakehouse.dbo.silver_orders o

CROSS JOIN replay_clock c

WHERE o.order_purchase_timestamp < c.cutoff_date
  AND o.order_status IN ('delivered', 'shipped');

GO