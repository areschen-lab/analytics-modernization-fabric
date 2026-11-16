SELECT
    f.order_id
FROM {{ ref('fact_lab_orders') }} AS f
LEFT JOIN {{ ref('dim_customer') }} AS c
    ON f.customer_id = c.customer_id
WHERE c.customer_id IS NULL
   OR f.customer_unique_id IS NULL
   OR f.customer_unique_id <> c.customer_unique_id