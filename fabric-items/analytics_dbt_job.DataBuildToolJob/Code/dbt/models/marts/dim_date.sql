
WITH date_range AS (
    SELECT
        CAST(MIN(order_purchase_timestamp) AS DATE) AS min_date,
        CAST(MAX(order_purchase_timestamp) AS DATE) AS max_date
    FROM {{ ref('stg_orders') }}
),

calendar AS (
    SELECT
        DATEADD(DAY, s.value, r.min_date) AS [date]
    FROM date_range r
    CROSS APPLY GENERATE_SERIES(
        0,
        DATEDIFF(DAY, r.min_date, r.max_date),
        1
    ) s
),

date_attributes AS (
    SELECT
        [date],
        ((DATEDIFF(DAY, CAST('19000107' AS DATE), [date])
          % 7 + 7) % 7) + 1 AS day_of_week
    FROM calendar
)

SELECT
    [date],
    YEAR([date]) AS [year],
    DATEPART(QUARTER, [date]) AS [quarter],
    MONTH([date]) AS [month],
    DATEFROMPARTS(
        YEAR([date]), MONTH([date]), 1
    ) AS month_start,

    CHOOSE(MONTH([date]),
        'January', 'February', 'March', 'April',
        'May', 'June', 'July', 'August',
        'September', 'October', 'November', 'December'
    ) AS month_name,

    DAY([date]) AS [day],
    day_of_week,

    CHOOSE(day_of_week,
        'Sunday', 'Monday', 'Tuesday', 'Wednesday',
        'Thursday', 'Friday', 'Saturday'
    ) AS day_name

FROM date_attributes
