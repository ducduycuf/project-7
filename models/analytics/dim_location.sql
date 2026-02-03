{{ config(materialized='table') }}

WITH dim_location__source AS (
    SELECT 
        CASE 
            WHEN country IS NULL OR TRIM(country) = '' OR country = '-' THEN 'unknown'
            ELSE LOWER(TRIM(country))
        END AS country_name
        ,CASE 
            WHEN region IS NULL OR TRIM(region) = '' OR region = '-' THEN 'unknown'
            ELSE LOWER(TRIM(region))
        END AS region_name
        ,CASE 
            WHEN city IS NULL OR TRIM(city) = '' OR city = '-' THEN 'unknown'
            ELSE LOWER(TRIM(city))
        END AS city_name
        ,ip_address
    FROM {{ ref('stg_location') }}
    WHERE ip_address IS NOT NULL
)

,dim_location__aggregated AS (
    SELECT 
        country_name
        ,region_name
        ,city_name
        ,COUNT(DISTINCT ip_address) AS ip_count
    FROM dim_location__source
    GROUP BY country_name, region_name, city_name
)

,dim_location__transformed AS (
    SELECT
        FARM_FINGERPRINT(CONCAT(country_name, '|', region_name, '|', city_name)) AS location_key
        ,CONCAT(country_name, '|', region_name, '|', city_name) AS location_id
        ,country_name
        ,region_name
        ,city_name
        ,COALESCE(ip_count, 0) AS ip_count
    FROM dim_location__aggregated
)

,dim_location__undefined AS (
    SELECT
        CAST(-1 AS INT64) AS location_key
        ,'undefined|undefined|undefined' AS location_id
        ,'undefined' AS country_name
        ,'undefined' AS region_name
        ,'undefined' AS city_name
        ,CAST(0 AS INT64) AS ip_count
)

,dim_location__final AS (
    SELECT * FROM dim_location__undefined
    UNION ALL
    SELECT * FROM dim_location__transformed
)

SELECT * FROM dim_location__final
