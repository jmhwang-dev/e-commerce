WITH 
-- [배치 우선] Order Detail 병합
merged_order_detail AS (
    SELECT order_id, product_id
    FROM gold.batch.fact_order_detail
    UNION ALL
    SELECT s.order_id, s.product_id
    FROM gold.stream.fact_order_detail s
    LEFT ANTI JOIN gold.batch.fact_order_detail b 
        ON s.order_id = b.order_id AND s.product_id = b.product_id AND s.seller_id = b.seller_id
),

-- [예외: 상태 기반 우선순위] Lead Days 병합
merged_lead_days AS (
    -- 1. 배송 완료된 건은 '배치'가 진실 (Priority 1)
    SELECT 
        order_id, 
        -- [추가됨] 날짜 관련 컬럼 확보
        purchase, approve, shipping_limit, delivered_carrier, estimated_delivery, delivered_customer,
        purchase_to_approval_days, approval_to_carrier_days, 
        carrier_to_customer_days, carrier_delivery_delay_days, customer_delivery_delay_days
    FROM gold.batch.fact_order_lead_days
    WHERE delivered_customer IS NOT NULL

    UNION ALL
    
    -- 2. 배송 미완료 건은 '스트림'이 최신 상태 (Priority 2)
    -- 단, 위에서 이미 가져온(완료된) 주문 ID는 제외
    SELECT 
        s.order_id, 
        -- [추가됨] 날짜 관련 컬럼 확보
        s.purchase, s.approve, s.shipping_limit, s.delivered_carrier, s.estimated_delivery, s.delivered_customer,
        s.purchase_to_approval_days, s.approval_to_carrier_days, 
        s.carrier_to_customer_days, s.carrier_delivery_delay_days, s.customer_delivery_delay_days
    FROM gold.stream.fact_order_lead_days s
    LEFT ANTI JOIN gold.batch.fact_order_lead_days b ON s.order_id = b.order_id AND b.delivered_customer IS NOT NULL
    
    UNION ALL
    
    -- 3. 스트림에 없는 잔여 건은 '배치'에서 가져옴 (Priority 3)
    SELECT 
        b.order_id, 
        -- [추가됨] 날짜 관련 컬럼 확보
        b.purchase, b.approve, b.shipping_limit, b.delivered_carrier, b.estimated_delivery, b.delivered_customer,
        b.purchase_to_approval_days, b.approval_to_carrier_days, 
        b.carrier_to_customer_days, b.carrier_delivery_delay_days, b.customer_delivery_delay_days
    FROM gold.batch.fact_order_lead_days b
    LEFT ANTI JOIN gold.stream.fact_order_lead_days s ON b.order_id = s.order_id
    WHERE b.delivered_customer IS NULL
),

-- [배치 전용] 리드타임 데이터 포맷팅
LeadDaysWithSalesPeriod AS (
    SELECT
        order_id,
        -- [추가됨] 상위 쿼리로 전달
        purchase, approve, shipping_limit, delivered_carrier, estimated_delivery, delivered_customer,
        date_format(delivered_customer, 'yyyy-MM') AS sales_period,
        purchase_to_approval_days,
        approval_to_carrier_days,
        carrier_to_customer_days,
        carrier_delivery_delay_days,
        customer_delivery_delay_days
    FROM merged_lead_days
    WHERE delivered_customer IS NOT NULL
),

-- [배치 전용] 월별 상품 그룹
ProductGroupByPeriod AS (
    SELECT product_id, category, group, sales_period
    FROM gold.batch.fact_monthly_sales_by_product
)

SELECT *
FROM merged_order_detail od
JOIN LeadDaysWithSalesPeriod ld
    USING(order_id)
JOIN ProductGroupByPeriod pg
    USING(product_id, sales_period);