{{ config(
    materialized='table',
    file_format='iceberg',
    database='warehouse_dev',
    schema='gold'
) }}

select *
from {{ ref('stg_diff_delivery') }}