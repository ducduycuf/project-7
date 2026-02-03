{{ config(materialized='view') }}

WITH stg_location__source AS (
    SELECT *
    FROM {{ source('glamira_raw', 'ip_locations') }}
)

,stg_location__deduped AS (
    SELECT
        CAST(ip AS STRING) AS ip_address
        ,LOWER(TRIM(CAST(country AS STRING))) AS country
        ,LOWER(TRIM(CAST(region AS STRING))) AS region
        ,LOWER(TRIM(CAST(city AS STRING))) AS city
        ,ROW_NUMBER() OVER (
            PARTITION BY CAST(ip AS STRING)
            ORDER BY country, region, city
        ) AS rn
    FROM stg_location__source
    WHERE ip IS NOT NULL
        AND TRIM(CAST(ip AS STRING)) != ''
)

,stg_location__final AS (
    SELECT
        ip_address
        ,country
        ,region
        ,city
        ,CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM stg_location__deduped
    WHERE rn = 1
)

SELECT * FROM stg_location__final
