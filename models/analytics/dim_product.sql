{{ config(materialized='table') }}

WITH dim_product__source AS (
    SELECT
        product_id
        ,product_name
        ,category_name
        ,sku
        ,price
        ,store_code
        ,crawled_at
        ,dbt_loaded_at
        ,ROW_NUMBER() OVER (
            PARTITION BY product_id 
            ORDER BY crawled_at DESC, dbt_loaded_at DESC
        ) AS rn
    FROM {{ ref('stg_product') }}
    WHERE product_id IS NOT NULL
)

,dim_product__known AS (
    SELECT
        product_id
        ,product_name
        ,category_name
        ,sku
        ,price
        ,store_code
    FROM dim_product__source
    WHERE rn = 1
)

,dim_product__order_products AS (
    SELECT DISTINCT 
        CAST(product_id AS INT64) AS product_id
    FROM {{ ref('stg_order') }}
    WHERE product_id IS NOT NULL
)

,dim_product__missing AS (
    SELECT 
        op.product_id
    FROM dim_product__order_products op
    LEFT JOIN dim_product__known kp 
        ON op.product_id = kp.product_id
    WHERE kp.product_id IS NULL
)

,dim_product__combined AS (
    SELECT
        product_id
        ,product_name
        ,category_name
        ,sku
        ,price
        ,store_code
        ,FALSE AS is_missing_from_catalog
    FROM dim_product__known

    UNION ALL

    SELECT
        product_id
        ,CONCAT('[Unknown Product #', CAST(product_id AS STRING), ']') AS product_name
        ,'undefined' AS category_name
        ,'undefined' AS sku
        ,CAST(0 AS FLOAT64) AS price
        ,'undefined' AS store_code
        ,TRUE AS is_missing_from_catalog
    FROM dim_product__missing
)

,dim_product__transformed AS (
    SELECT
        FARM_FINGERPRINT(CAST(product_id AS STRING)) AS product_key
        ,product_id
        ,COALESCE(product_name, 'undefined') AS product_name
        ,COALESCE(category_name, 'undefined') AS category_name
        ,COALESCE(sku, 'undefined') AS product_sku
        ,COALESCE(store_code, 'undefined') AS store_code
        ,COALESCE(price, 0) AS unit_price
        ,is_missing_from_catalog
    FROM dim_product__combined
)

,dim_product__undefined AS (
    SELECT
        CAST(-1 AS INT64) AS product_key
        ,CAST(-1 AS INT64) AS product_id
        ,'undefined' AS product_name
        ,'undefined' AS category_name
        ,'undefined' AS product_sku
        ,'undefined' AS store_code
        ,CAST(0 AS FLOAT64) AS unit_price
        ,FALSE AS is_missing_from_catalog
)

,dim_product__final AS (
    SELECT * FROM dim_product__undefined
    UNION ALL
    SELECT * FROM dim_product__transformed
)

SELECT * FROM dim_product__final
