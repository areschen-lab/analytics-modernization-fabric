CREATE TABLE [dbt_dev].[dim_customer] (
    [customer_id]              VARCHAR (8000) NULL,
    [customer_unique_id]       VARCHAR (8000) NULL,
    [customer_zip_code_prefix] INT            NULL,
    [customer_city]            VARCHAR (8000) NULL,
    [customer_state]           VARCHAR (8000) NULL,
    [geo_city]                 VARCHAR (8000) NULL,
    [geo_state]                VARCHAR (8000) NULL,
    [latitude]                 FLOAT (53)     NULL,
    [longitude]                FLOAT (53)     NULL
);


GO