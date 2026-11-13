
SELECT
    review_id,
    order_id,
    review_score,
    review_creation_date
FROM dbt_lab.stg_lab_reviews
WHERE order_id = '0071ee2429bc1efdc43aa3e073a5290e';
