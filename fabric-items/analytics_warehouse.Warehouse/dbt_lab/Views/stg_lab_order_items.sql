create view [dbt_lab].[stg_lab_order_items] as 

SELECT i.*
FROM analytics_lakehouse.dbo.silver_order_items i

INNER JOIN [analytics_warehouse].[dbt_lab].[stg_lab_orders] o
    ON i.order_id = o.order_id;

GO