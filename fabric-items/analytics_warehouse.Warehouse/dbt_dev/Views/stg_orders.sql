create view [dbt_dev].[stg_orders] as SELECT
    *
FROM analytics_lakehouse.dbo.silver_orders;

GO