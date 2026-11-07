CREATE TABLE [dbt_dev].[dim_date] (
    [date]        DATE        NULL,
    [year]        INT         NULL,
    [quarter]     INT         NULL,
    [month]       INT         NULL,
    [month_start] DATE        NULL,
    [month_name]  VARCHAR (9) NULL,
    [day]         INT         NULL,
    [day_of_week] INT         NULL,
    [day_name]    VARCHAR (9) NULL
);


GO