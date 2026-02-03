{{ config(materialized='table') }}

WITH dim_date__generate AS (
    SELECT *
    FROM UNNEST(GENERATE_DATE_ARRAY('2010-01-01', '2031-01-01', INTERVAL 1 DAY)) AS dt
)

,dim_date__transformed AS (
    SELECT
        CAST(FORMAT_DATE('%Y%m%d', dt) AS INT64) AS date_key
        ,dt AS full_date
        ,EXTRACT(YEAR FROM dt) AS year_number
        ,EXTRACT(QUARTER FROM dt) AS quarter_number
        ,EXTRACT(MONTH FROM dt) AS month_number
        ,EXTRACT(WEEK FROM dt) AS week_of_year
        ,EXTRACT(DAY FROM dt) AS day_of_month
        ,EXTRACT(DAYOFWEEK FROM dt) AS day_of_week
        ,FORMAT_DATE('%A', dt) AS day_name
        ,FORMAT_DATE('%B', dt) AS month_name
        ,FORMAT_DATE('%Y-%m', dt) AS year_month
        ,FORMAT_DATE('%Y-Q%Q', dt) AS year_quarter
        ,CASE WHEN EXTRACT(DAYOFWEEK FROM dt) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend
    FROM dim_date__generate
)

,dim_date__undefined AS (
    SELECT
        CAST(-1 AS INT64) AS date_key
        ,CAST(NULL AS DATE) AS full_date
        ,CAST(-1 AS INT64) AS year_number
        ,CAST(-1 AS INT64) AS quarter_number
        ,CAST(-1 AS INT64) AS month_number
        ,CAST(-1 AS INT64) AS week_of_year
        ,CAST(-1 AS INT64) AS day_of_month
        ,CAST(-1 AS INT64) AS day_of_week
        ,'undefined' AS day_name
        ,'undefined' AS month_name
        ,'undefined' AS year_month
        ,'undefined' AS year_quarter
        ,FALSE AS is_weekend
)

,dim_date__final AS (
    SELECT * FROM dim_date__undefined
    UNION ALL
    SELECT * FROM dim_date__transformed
)

SELECT * FROM dim_date__final
ORDER BY date_key