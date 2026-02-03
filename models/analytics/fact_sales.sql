{{
    config(
        materialized='incremental'
        ,incremental_strategy='merge'
        ,unique_key='sales_key'
        ,on_schema_change='sync_all_columns'
    )
}}

WITH fact_sales__source AS (
    SELECT 
        order_id
        ,product_id
        ,user_id_db
        ,user_email
        ,device_id
        ,ip_address
        ,timestamp
        ,quantity
        ,unit_price
        ,store_id
        ,current_url
    FROM {{ ref('stg_order') }}
    {% if is_incremental() %}
        WHERE DATE(timestamp) >= DATE_ADD(CURRENT_DATE(), INTERVAL -3 DAY)
    {% endif %}
)

,fact_sales__with_domain AS (
    SELECT
        *
        ,COALESCE(
            REGEXP_EXTRACT(current_url, r'glamira\.com\.([a-z]+)')
            ,REGEXP_EXTRACT(current_url, r'glamira\.([a-z]+)')
        ) AS domain_raw
    FROM fact_sales__source
)

,fact_sales__with_currency AS (
    SELECT
        *
        ,CASE 
            WHEN REGEXP_CONTAINS(current_url, r'glamira\.com\.co') THEN STRUCT('COP' AS currency_code, 0.00025 AS rate)
            WHEN REGEXP_CONTAINS(current_url, r'glamira\.com\.py') THEN STRUCT('PYG' AS currency_code, 0.00013 AS rate)
            WHEN REGEXP_CONTAINS(current_url, r'glamira\.com\.pe') THEN STRUCT('PEN' AS currency_code, 0.27 AS rate)
            WHEN REGEXP_CONTAINS(current_url, r'glamira\.com\.ar') THEN STRUCT('ARS' AS currency_code, 0.0012 AS rate)
            WHEN REGEXP_CONTAINS(current_url, r'glamira\.com\.mx') THEN STRUCT('MXN' AS currency_code, 0.059 AS rate)
            WHEN REGEXP_CONTAINS(current_url, r'glamira\.com\.br') THEN STRUCT('BRL' AS currency_code, 0.20 AS rate)
            WHEN domain_raw = 'com' AND NOT REGEXP_CONTAINS(current_url, r'glamira\.com\.[a-z]+') 
                THEN STRUCT('USD' AS currency_code, 1.0 AS rate)
            WHEN domain_raw = 'vn' THEN STRUCT('VND' AS currency_code, 0.000041 AS rate)
            WHEN domain_raw = 'jp' THEN STRUCT('JPY' AS currency_code, 0.0067 AS rate)
            WHEN domain_raw = 'in' THEN STRUCT('INR' AS currency_code, 0.012 AS rate)
            WHEN domain_raw = 'ae' THEN STRUCT('AED' AS currency_code, 0.27 AS rate)
            WHEN domain_raw = 'hk' THEN STRUCT('HKD' AS currency_code, 0.13 AS rate)
            WHEN domain_raw = 'rs' THEN STRUCT('RSD' AS currency_code, 0.0093 AS rate)
            WHEN domain_raw = 'md' THEN STRUCT('MDL' AS currency_code, 0.056 AS rate)
            WHEN domain_raw IN ('de', 'fr', 'it', 'es', 'nl', 'be', 'at', 'ie', 'pt', 'fi', 'sk', 'lt', 'ee', 'si', 'hr')
                THEN STRUCT('EUR' AS currency_code, 1.09 AS rate)
            WHEN domain_raw = 'co' AND NOT REGEXP_CONTAINS(current_url, r'glamira\.com\.co')
                THEN STRUCT('GBP' AS currency_code, 1.27 AS rate)
            WHEN domain_raw = 'ch' THEN STRUCT('CHF' AS currency_code, 1.12 AS rate)
            WHEN domain_raw = 'se' THEN STRUCT('SEK' AS currency_code, 0.095 AS rate)
            WHEN domain_raw = 'dk' THEN STRUCT('DKK' AS currency_code, 0.15 AS rate)
            WHEN domain_raw = 'no' THEN STRUCT('NOK' AS currency_code, 0.094 AS rate)
            WHEN domain_raw = 'pl' THEN STRUCT('PLN' AS currency_code, 0.25 AS rate)
            WHEN domain_raw = 'cz' THEN STRUCT('CZK' AS currency_code, 0.044 AS rate)
            WHEN domain_raw = 'hu' THEN STRUCT('HUF' AS currency_code, 0.0028 AS rate)
            WHEN domain_raw = 'ro' THEN STRUCT('RON' AS currency_code, 0.22 AS rate)
            WHEN domain_raw = 'bg' THEN STRUCT('BGN' AS currency_code, 0.56 AS rate)
            WHEN domain_raw = 'ca' THEN STRUCT('CAD' AS currency_code, 0.74 AS rate)
            WHEN domain_raw = 'cl' THEN STRUCT('CLP' AS currency_code, 0.0011 AS rate)
            WHEN domain_raw = 'mx' THEN STRUCT('MXN' AS currency_code, 0.059 AS rate)
            WHEN domain_raw = 'br' THEN STRUCT('BRL' AS currency_code, 0.20 AS rate)
            WHEN domain_raw = 'sg' THEN STRUCT('SGD' AS currency_code, 0.74 AS rate)
            WHEN domain_raw = 'au' THEN STRUCT('AUD' AS currency_code, 0.66 AS rate)
            WHEN domain_raw = 'nz' THEN STRUCT('NZD' AS currency_code, 0.61 AS rate)
            WHEN domain_raw = 'kr' THEN STRUCT('KRW' AS currency_code, 0.00075 AS rate)
            WHEN domain_raw = 'th' THEN STRUCT('THB' AS currency_code, 0.029 AS rate)
            WHEN domain_raw = 'my' THEN STRUCT('MYR' AS currency_code, 0.22 AS rate)
            WHEN domain_raw = 'ph' THEN STRUCT('PHP' AS currency_code, 0.018 AS rate)
            WHEN domain_raw = 'id' THEN STRUCT('IDR' AS currency_code, 0.000063 AS rate)
            WHEN domain_raw = 'tw' THEN STRUCT('TWD' AS currency_code, 0.031 AS rate)
            WHEN domain_raw = 'cn' THEN STRUCT('CNY' AS currency_code, 0.14 AS rate)
            WHEN domain_raw = 'tr' THEN STRUCT('TRY' AS currency_code, 0.038 AS rate)
            WHEN domain_raw = 'il' THEN STRUCT('ILS' AS currency_code, 0.28 AS rate)
            WHEN domain_raw = 'za' THEN STRUCT('ZAR' AS currency_code, 0.055 AS rate)
            WHEN domain_raw = 'sa' THEN STRUCT('SAR' AS currency_code, 0.27 AS rate)
            ELSE STRUCT('USD' AS currency_code, 1.0 AS rate)
        END AS currency_info
    FROM fact_sales__with_domain
)

,fact_sales__locations AS (
    SELECT
        ip_address
        ,CASE 
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
    FROM {{ ref('stg_location') }}
)

,fact_sales__joined AS (
    SELECT
        FARM_FINGERPRINT(CONCAT(f.order_id, '|', CAST(f.product_id AS STRING))) AS sales_key
        
        ,COALESCE(
            FARM_FINGERPRINT(CONCAT(
                COALESCE(f.user_id_db, COALESCE(f.device_id, 'undefined'))
                ,'|'
                ,COALESCE(f.user_email, 'undefined')
            ))
            ,-1
        ) AS customer_key
        
        ,COALESCE(FARM_FINGERPRINT(CAST(f.product_id AS STRING)), -1) AS product_key
        
        ,COALESCE(CAST(FORMAT_DATE('%Y%m%d', DATE(f.timestamp)) AS INT64), -1) AS date_key
        
        ,COALESCE(
            FARM_FINGERPRINT(CONCAT(
                COALESCE(l.country_name, 'unknown')
                ,'|'
                ,COALESCE(l.region_name, 'unknown')
                ,'|'
                ,COALESCE(l.city_name, 'unknown')
            ))
            ,-1
        ) AS location_key
        
        ,f.order_id
        ,COALESCE(f.store_id, 'undefined') AS store_id
        ,COALESCE(f.currency_info.currency_code, 'USD') AS currency_code
        
        ,COALESCE(f.quantity, 0) AS order_qty
        ,COALESCE(ROUND(f.unit_price, 2), 0) AS unit_price_local
        ,COALESCE(ROUND(f.quantity * f.unit_price, 2), 0) AS sales_amount_local
        ,COALESCE(f.currency_info.rate, 1.0) AS exchange_rate
        ,COALESCE(ROUND((f.unit_price * f.currency_info.rate) * f.quantity, 2), 0) AS sales_amount_usd
        
        ,DATE(f.timestamp) AS order_date
        ,f.timestamp AS order_timestamp

    FROM fact_sales__with_currency f
    LEFT JOIN fact_sales__locations l 
        ON f.ip_address = l.ip_address
    WHERE f.order_id IS NOT NULL
        AND TRIM(f.order_id) != ''
        AND f.product_id IS NOT NULL
)

-- Final dedup: keep only 1 row per sales_key
,fact_sales__final AS (
    SELECT *
    FROM (
        SELECT 
            *
            ,ROW_NUMBER() OVER (
                PARTITION BY sales_key 
                ORDER BY order_timestamp DESC, order_qty DESC, sales_amount_usd DESC
            ) AS final_row_num
        FROM fact_sales__joined
    )
    WHERE final_row_num = 1
)

SELECT 
    sales_key
    ,customer_key
    ,product_key
    ,date_key
    ,location_key
    ,order_id
    ,store_id
    ,currency_code
    ,order_qty
    ,unit_price_local
    ,sales_amount_local
    ,exchange_rate
    ,sales_amount_usd
    ,order_date
    ,order_timestamp
FROM fact_sales__final