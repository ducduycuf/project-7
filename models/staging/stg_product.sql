{{ config(materialized='view') }}

WITH stg_product__source AS (
    SELECT
        react_fields.product_id AS product_id
        ,react_fields.name AS product_name
        ,react_fields.sku
        ,react_fields.store_code
        ,react_fields.category_name
        ,react_fields.price AS raw_price
        ,crawled_at
        ,status
    FROM {{ source('glamira_raw', 'product_collection') }}
    WHERE react_fields.product_id IS NOT NULL
        AND react_fields.name IS NOT NULL
        AND status = 'OK'
)

,stg_product__final AS (
    SELECT
        CAST(product_id AS INT64) AS product_id
        ,product_name
        ,sku
        ,store_code
        ,category_name
        ,CAST(
            CASE
                WHEN CAST(raw_price AS STRING) LIKE '%.%,%' THEN
                    REGEXP_REPLACE(REGEXP_REPLACE(CAST(raw_price AS STRING), r'\.', ''), r',', '.')
                WHEN CAST(raw_price AS STRING) LIKE '%,%' AND NOT CAST(raw_price AS STRING) LIKE '%.%' THEN
                    REGEXP_REPLACE(CAST(raw_price AS STRING), r',', '.')
                WHEN CAST(raw_price AS STRING) LIKE '%,%' AND CAST(raw_price AS STRING) LIKE '%.%' THEN
                    REGEXP_REPLACE(CAST(raw_price AS STRING), r',', '')
                ELSE
                    CAST(raw_price AS STRING)
            END AS FLOAT64
        ) AS price
        ,CAST(crawled_at AS TIMESTAMP) AS crawled_at
        ,status
        ,CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM stg_product__source
)

SELECT * FROM stg_product__final
