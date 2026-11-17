WITH main_data AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        price,
        freight_value,
        item_total
    FROM dbt_dev.fact_order_items
),
lab_data AS (
    SELECT
        order_id,
        order_item_id,
        product_id,
        seller_id,
        price,
        freight_value,
        item_total
    FROM dbt_lab.fact_lab_order_items
),
only_main AS (
    SELECT * FROM main_data
    EXCEPT
    SELECT * FROM lab_data
),
only_lab AS (
    SELECT * FROM lab_data
    EXCEPT
    SELECT * FROM main_data
)
SELECT
    (SELECT COUNT_BIG(*) FROM main_data) AS main_rows,
    (SELECT COUNT_BIG(*) FROM lab_data) AS lab_rows,
    (SELECT COUNT_BIG(*) FROM only_main) AS only_main,
    (SELECT COUNT_BIG(*) FROM only_lab) AS only_lab;