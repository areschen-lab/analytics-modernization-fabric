CREATE TABLE [dbt_dev].[fact_orders] (
    [order_id]                      VARCHAR (8000) NULL,
    [customer_id]                   VARCHAR (8000) NULL,
    [customer_unique_id]            VARCHAR (8000) NULL,
    [order_status]                  VARCHAR (8000) NULL,
    [order_purchase_timestamp]      DATETIME2 (6)  NULL,
    [order_purchase_date]           DATE           NULL,
    [order_delivered_customer_date] DATETIME2 (6)  NULL,
    [order_estimated_delivery_date] DATETIME2 (6)  NULL,
    [delivery_days]                 INT            NULL,
    [delay_days]                    INT            NULL,
    [is_late]                       INT            NULL,
    [payment_total]                 FLOAT (53)     NULL,
    [payment_count]                 INT            NULL,
    [review_id]                     VARCHAR (8000) NULL,
    [review_score]                  INT            NULL,
    [review_comment_title]          VARCHAR (8000) NULL,
    [review_comment_message]        VARCHAR (8000) NULL,
    [review_creation_date]          DATETIME2 (6)  NULL,
    [review_answer_timestamp]       DATETIME2 (6)  NULL
);


GO