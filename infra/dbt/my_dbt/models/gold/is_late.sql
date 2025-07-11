SELECT
    count(order_id),
    count(customer_id),
    count(is_late)
FROM {{ source('silver_orders', 'diff_delivery_date') }}
