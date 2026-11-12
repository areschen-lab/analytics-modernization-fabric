CREATE TABLE [dbt_lab].[delayed_records] (
    [record_type]        VARCHAR (20) NOT NULL,
    [order_id]           VARCHAR (32) NOT NULL,
    [payment_sequential] INT          NULL,
    [review_id]          VARCHAR (32) NULL,
    [available_batch]    INT          NOT NULL
);


GO