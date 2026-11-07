create view [dbt_dev].[int_reviews_canonical] as WITH ranked_reviews AS (
    SELECT
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp,

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

    FROM analytics_lakehouse.dbo.silver_order_reviews
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
WHERE rn = 1;

GO