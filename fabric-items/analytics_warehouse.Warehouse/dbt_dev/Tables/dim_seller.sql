CREATE TABLE [dbt_dev].[dim_seller] (
    [seller_id]              VARCHAR (8000) NULL,
    [seller_zip_code_prefix] INT            NULL,
    [seller_city]            VARCHAR (8000) NULL,
    [seller_state]           VARCHAR (8000) NULL,
    [geo_city]               VARCHAR (8000) NULL,
    [geo_state]              VARCHAR (8000) NULL,
    [latitude]               FLOAT (53)     NULL,
    [longitude]              FLOAT (53)     NULL
);


GO