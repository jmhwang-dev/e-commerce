WITH 
-- [배치 우선] Order Detail 병합
-- 배치를 먼저 다 가져오고, 스트림은 배치에 없는 것만 끼워넣음
merged_order_detail AS (
    SELECT order_id, customer_id, seller_id, product_id, category, quantity, unit_price
    FROM gold.batch.fact_order_detail
    WHERE order_id IS NOT NULL

    UNION ALL

    SELECT s.order_id, s.customer_id, s.seller_id, s.product_id, s.category, s.quantity, s.unit_price
    FROM gold.stream.fact_order_detail s
    LEFT ANTI JOIN gold.batch.fact_order_detail b 
        ON s.order_id = b.order_id AND s.product_id = b.product_id AND s.seller_id = b.seller_id
    WHERE s.order_id IS NOT NULL
),

-- [배치 전용] 월별 판매 그룹 (스트림 없음)
LatestProductPorfolioGroup AS (
    SELECT
        product_id,
        group,
        rank() OVER(PARTITION BY product_id ORDER BY sales_period DESC) AS rnk
    FROM gold.batch.fact_monthly_sales_by_product
)

SELECT
    od.product_id,
    od.order_id,
    od.customer_id,
    od.seller_id,
    od.category,
    od.quantity,
    od.unit_price,
    lg.group AS latest_group
FROM merged_order_detail od
LEFT JOIN LatestProductPorfolioGroup lg 
    ON od.product_id = lg.product_id AND lg.rnk = 1;