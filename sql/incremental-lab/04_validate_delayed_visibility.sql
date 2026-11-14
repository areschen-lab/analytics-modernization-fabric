
-- Validate all delayed payments and reviews in Batch 0

SELECT
    d.record_type,
    d.available_batch,
    COUNT(*) AS delayed_records,
    SUM(
        CASE
            WHEN d.record_type = 'payment'
                 AND p.order_id IS NOT NULL THEN 1
            WHEN d.record_type = 'review'
                 AND r.order_id IS NOT NULL THEN 1
            ELSE 0
        END
    ) AS incorrectly_visible
FROM dbt_lab.delayed_records d

LEFT JOIN dbt_lab.stg_lab_payments p
    ON d.record_type = 'payment'
    AND d.order_id = p.order_id
    AND d.payment_sequential = p.payment_sequential

LEFT JOIN dbt_lab.stg_lab_reviews r
    ON d.record_type = 'review'
    AND d.order_id = r.order_id
    AND d.review_id = r.review_id

GROUP BY
    d.record_type,
    d.available_batch

ORDER BY
    d.record_type,
    d.available_batch;
