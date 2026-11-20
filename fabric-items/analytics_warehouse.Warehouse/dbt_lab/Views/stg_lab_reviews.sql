create view [dbt_lab].[stg_lab_reviews] as 

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

SELECT r.*
FROM analytics_lakehouse.dbo.silver_order_reviews r

INNER JOIN [analytics_warehouse].[dbt_lab].[stg_lab_orders] o
    ON r.order_id = o.order_id

CROSS JOIN replay_clock c

WHERE r.review_answer_timestamp < c.cutoff_date;

GO