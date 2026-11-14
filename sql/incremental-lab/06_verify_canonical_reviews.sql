
-- Check 1: Each order has at most one canonical review
SELECT
    order_id,
    COUNT_BIG(*) AS review_count
FROM dbt_lab.int_lab_reviews_canonical
GROUP BY order_id
HAVING COUNT_BIG(*) > 1;

-- Check 2: Known delayed review remains hidden
SELECT
    order_id,
    review_id,
    review_score
FROM dbt_lab.int_lab_reviews_canonical
WHERE order_id = '0071ee2429bc1efdc43aa3e073a5290e';
