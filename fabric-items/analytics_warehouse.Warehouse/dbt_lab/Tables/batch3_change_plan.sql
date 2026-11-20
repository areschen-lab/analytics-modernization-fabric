CREATE TABLE [dbt_lab].[batch3_change_plan] (
    [order_id]        VARCHAR (32) NOT NULL,
    [change_order]    INT          NOT NULL,
    [change_payment]  INT          NOT NULL,
    [change_review]   INT          NOT NULL,
    [change_item]     INT          NOT NULL,
    [available_batch] INT          NOT NULL
);


GO