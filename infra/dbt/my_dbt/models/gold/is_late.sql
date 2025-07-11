{{ config(
    schema='gold',
    materialized='table'
) }}

SELECT
    order_id,
    customer_id,
    is_late
FROM {{ source('silver_orders', 'diff_delivery_date') }}
