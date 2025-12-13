-- A. [Batch] 배치의 미배송 건 (이것이 실제 지연 현황)
SELECT 
    order_id, 
    purchase, approve, shipping_limit, delivered_carrier, estimated_delivery, delivered_customer,
    purchase_to_approval_days, approval_to_carrier_days, 
    carrier_to_customer_days, carrier_delivery_delay_days, customer_delivery_delay_days
FROM gold.batch.fact_order_lead_days
WHERE delivered_customer IS NULL -- 아직 도착 안 함 (실제 상황)

UNION ALL

-- B. [Stream] 스트림의 미배송 건 (배치에 아직 안 들어온 신규 건)
-- 주의: 배치 테이블 전체(b)와 비교해서 없는 것만 가져옴.
-- (배치에 이미 있다면, 완료됐든 미완료든 배치가 Fact이므로 스트림의 시뮬레이션 값은 무시)
SELECT 
    s.order_id, 
    s.purchase, s.approve, s.shipping_limit, s.delivered_carrier, s.estimated_delivery, s.delivered_customer,
    s.purchase_to_approval_days, s.approval_to_carrier_days, 
    s.carrier_to_customer_days, s.carrier_delivery_delay_days, s.customer_delivery_delay_days
FROM gold.stream.fact_order_lead_days s
LEFT ANTI JOIN gold.batch.fact_order_lead_days b ON s.order_id = b.order_id
WHERE s.delivered_customer IS NULL