{{ config(materialized='table') }}

WITH dim_customer__source AS (
    SELECT 
        COALESCE(CAST(user_id_db AS STRING), 'undefined') AS customer_id
        ,COALESCE(LOWER(TRIM(CAST(user_email AS STRING))), 'undefined') AS email_address
        ,COALESCE(device_id, 'undefined') AS device_id
        ,COALESCE(ip_address, 'undefined') AS last_ip_address
        ,timestamp
        ,order_id
    FROM {{ ref('stg_order') }}
)

,dim_customer__aggregated AS (
    SELECT 
        customer_id
        ,email_address
        ,ARRAY_AGG(device_id ORDER BY timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS device_id
        ,ARRAY_AGG(last_ip_address ORDER BY timestamp DESC LIMIT 1)[SAFE_OFFSET(0)] AS last_ip_address
        ,MIN(timestamp) AS first_order_date
        ,MAX(timestamp) AS last_order_date
        ,COUNT(DISTINCT order_id) AS total_orders
    FROM dim_customer__source
    GROUP BY customer_id, email_address
)

,dim_customer__ranked AS (
    SELECT 
        *
        ,ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY last_order_date DESC
        ) AS recency_rank
        ,LEAD(first_order_date) OVER (
            PARTITION BY customer_id 
            ORDER BY first_order_date ASC
        ) AS next_email_start
    FROM dim_customer__aggregated
)

,dim_customer__transformed AS (
    SELECT
        FARM_FINGERPRINT(CONCAT(customer_id, '|', email_address)) AS customer_key
        ,customer_id
        ,email_address
        ,device_id
        ,last_ip_address
        ,first_order_date AS valid_from_date
        ,next_email_start AS valid_to_date
        ,CASE WHEN recency_rank = 1 THEN TRUE ELSE FALSE END AS is_current
        ,first_order_date
        ,last_order_date
        ,COALESCE(total_orders, 0) AS total_orders
        ,CASE WHEN customer_id = 'undefined' THEN TRUE ELSE FALSE END AS is_anonymous
    FROM dim_customer__ranked
)

,dim_customer__undefined AS (
    SELECT
        CAST(-1 AS INT64) AS customer_key
        ,'undefined' AS customer_id
        ,'undefined' AS email_address
        ,'undefined' AS device_id
        ,'undefined' AS last_ip_address
        ,CAST(NULL AS TIMESTAMP) AS valid_from_date
        ,CAST(NULL AS TIMESTAMP) AS valid_to_date
        ,TRUE AS is_current
        ,CAST(NULL AS TIMESTAMP) AS first_order_date
        ,CAST(NULL AS TIMESTAMP) AS last_order_date
        ,0 AS total_orders
        ,TRUE AS is_anonymous
)

,dim_customer__final AS (
    SELECT * FROM dim_customer__undefined
    UNION ALL
    SELECT * FROM dim_customer__transformed
)

SELECT * FROM dim_customer__final
