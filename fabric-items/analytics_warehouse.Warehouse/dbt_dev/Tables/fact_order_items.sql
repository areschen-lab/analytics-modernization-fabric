CREATE TABLE [dbt_dev].[fact_order_items] (
    [order_id]      VARCHAR (8000) NULL,
    [order_item_id] INT            NULL,
    [product_id]    VARCHAR (8000) NULL,
    [seller_id]     VARCHAR (8000) NULL,
    [price]         FLOAT (53)     NULL,
    [freight_value] FLOAT (53)     NULL,
    [item_total]    FLOAT (53)     NULL
);


GO