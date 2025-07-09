{{ config(
    materialized='view',
    database='warehouse_dev',
    schema='staging'
) }}

select *
from {{ source('silver_features', 'diff_delivery') }}