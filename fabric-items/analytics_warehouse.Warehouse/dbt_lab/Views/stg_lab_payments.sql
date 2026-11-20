create view [dbt_lab].[stg_lab_payments] as 

SELECT p.*
FROM analytics_lakehouse.dbo.silver_order_payments p

INNER JOIN [analytics_warehouse].[dbt_lab].[stg_lab_orders] o
    ON p.order_id = o.order_id;

GO