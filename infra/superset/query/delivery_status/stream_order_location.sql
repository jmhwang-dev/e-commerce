WITH 
-- [배치 우선] Order Detail 병합
merged_order_detail AS (
    SELECT order_id, customer_id, seller_id
    FROM gold.batch.fact_order_detail
    UNION ALL
    SELECT s.order_id, s.customer_id, s.seller_id
    FROM gold.stream.fact_order_detail s
    LEFT ANTI JOIN gold.batch.fact_order_detail b 
        ON s.order_id = b.order_id AND s.product_id = b.product_id AND s.seller_id = b.seller_id
),

-- [배치 우선] User Location 병합
-- 배치에 있는 유저면 배치를 쓰고, 배치에 없어야만 스트림을 씀
merged_user_location AS (
    SELECT user_type, user_id, lat, lng
    FROM gold.batch.dim_user_location
    WHERE lat IS NOT NULL AND lng IS NOT NULL
    
    UNION ALL
    
    SELECT s.user_type, s.user_id, s.lat, s.lng
    FROM gold.stream.dim_user_location s
    LEFT ANTI JOIN gold.batch.dim_user_location b 
        ON s.user_type = b.user_type AND s.user_id = b.user_id
    WHERE s.lat IS NOT NULL AND s.lng IS NOT NULL
)

SELECT
    od.order_id,
    od.customer_id AS user_id,
    ul.lng,
    ul.lat,
    'customer' AS user_type
FROM merged_order_detail od
JOIN merged_user_location ul 
    ON od.customer_id = ul.user_id

UNION ALL

SELECT
    od.order_id,
    od.seller_id AS user_id,
    ul.lng,
    ul.lat,
    'seller' AS user_type
FROM merged_order_detail od
JOIN merged_user_location ul 
    ON od.seller_id = ul.user_id;