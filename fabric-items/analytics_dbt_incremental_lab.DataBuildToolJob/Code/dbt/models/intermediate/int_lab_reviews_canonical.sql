
{{ config(materialized='view') }}

WITH ranked_reviews AS (
    SELECT
        r.*,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY
                CASE WHEN review_answer_timestamp IS NULL
                     THEN 1 ELSE 0 END,
                review_answer_timestamp ASC,
                CASE WHEN review_creation_date IS NULL
                     THEN 1 ELSE 0 END,
                review_creation_date ASC,
                review_id ASC
        ) AS rn
    FROM {{ ref('stg_lab_reviews') }} r
)

SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
FROM ranked_reviews
WHERE rn = 1
