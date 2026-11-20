CREATE TABLE [dbt_lab].[fact_lab_order_items] (
    [order_id]      VARCHAR (8000)  NULL,
    [order_item_id] INT             NULL,
    [product_id]    VARCHAR (8000)  NULL,
    [seller_id]     VARCHAR (8000)  NULL,
    [price]         DECIMAL (18, 2) NULL,
    [freight_value] DECIMAL (18, 2) NULL,
    [item_total]    DECIMAL (19, 2) NULL
);


GO