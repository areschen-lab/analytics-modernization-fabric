CREATE TABLE [dbt_dev].[dim_product] (
    [product_id]                 VARCHAR (8000) NULL,
    [product_category]           VARCHAR (8000) NULL,
    [product_category_name]      VARCHAR (8000) NULL,
    [product_name_lenght]        INT            NULL,
    [product_description_lenght] INT            NULL,
    [product_photos_qty]         INT            NULL,
    [product_weight_g]           INT            NULL,
    [product_length_cm]          INT            NULL,
    [product_height_cm]          INT            NULL,
    [product_width_cm]           INT            NULL
);


GO