WITH 
-- [배치 우선] Order Detail 병합
stream_order_detail AS (
    SELECT order_id, customer_id, seller_id
    FROM gold.stream.fact_order_detail
),

sream_user_location AS (
    SELECT user_type, user_id, lat, lng
    FROM gold.stream.dim_user_location
    WHERE lat IS NOT NULL AND lng IS NOT NULL
)

SELECT
    od.order_id,
    od.customer_id AS user_id,
    ul.lng,
    ul.lat,
    'customer' AS user_type
FROM stream_order_detail od
JOIN sream_user_location ul 
    ON od.customer_id = ul.user_id

UNION ALL

SELECT
    od.order_id,
    od.seller_id AS user_id,
    ul.lng,
    ul.lat,
    'seller' AS user_type
FROM stream_order_detail od
JOIN sream_user_location ul 
    ON od.seller_id = ul.user_id;