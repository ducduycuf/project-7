{{ config(materialized='view') }}

WITH stg_order__source AS (
    SELECT *
    FROM {{ source('glamira_raw', 'main_collection') }}
    WHERE collection = 'checkout_success'
)

,stg_order__flatten AS (
    SELECT
        _id
        ,order_id
        ,user_id_db
        ,email_address
        ,ip
        ,store_id
        ,device_id
        ,referrer_url
        ,currency
        ,time_stamp
        ,local_time
        ,current_url
        ,cp.element.product_id AS product_id
        ,cp.element.price AS raw_price
        ,cp.element.amount AS raw_quantity
        ,CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM stg_order__source
        ,UNNEST(cart_products.list) AS cp
)

,stg_order__price_cleaned AS (
    SELECT
        *
        ,REGEXP_REPLACE(CAST(raw_price AS STRING), r'[^0-9\.,]', '') AS price_str
    FROM stg_order__flatten
)

,stg_order__price_normalized AS (
    SELECT
        *
        ,CASE
            WHEN price_str IS NULL OR price_str = '' THEN NULL
            WHEN REGEXP_CONTAINS(price_str, r',\d{2}$') THEN
                REPLACE(REPLACE(price_str, '.', ''), ',', '.')
            ELSE
                REPLACE(price_str, ',', '')
        END AS price_final
    FROM stg_order__price_cleaned
)

,stg_order__final AS (
    SELECT
        _id AS event_id
        ,order_id
        ,CAST(product_id AS STRING) AS product_id
        ,CAST(user_id_db AS STRING) AS user_id_db
        ,CAST(time_stamp AS INT64) AS event_timestamp
        ,TIMESTAMP_SECONDS(CAST(time_stamp AS INT64)) AS timestamp
        ,CAST(local_time AS STRING) AS local_time
        ,COALESCE(CAST(raw_quantity AS INT64), 1) AS quantity
        ,COALESCE(SAFE_CAST(price_final AS FLOAT64), 0) AS unit_price
        ,CAST(ip AS STRING) AS ip_address
        ,LOWER(TRIM(CAST(email_address AS STRING))) AS user_email
        ,CAST(email_address AS STRING) AS customer_email
        ,CAST(store_id AS STRING) AS store_id
        ,referrer_url
        ,current_url
        ,CAST(device_id AS STRING) AS device_id
        ,CAST(currency AS STRING) AS currency
        ,dbt_loaded_at
        ,COALESCE(CAST(raw_quantity AS INT64), 1) * COALESCE(SAFE_CAST(price_final AS FLOAT64), 0) AS amount
    FROM stg_order__price_normalized
    WHERE order_id IS NOT NULL
        AND TRIM(order_id) != ''
        AND product_id IS NOT NULL
)

SELECT * FROM stg_order__final
