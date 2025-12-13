SELECT 
    s.order_id, 
    s.purchase, s.approve, s.shipping_limit, s.delivered_carrier, s.estimated_delivery, s.delivered_customer,
    s.purchase_to_approval_days, s.approval_to_carrier_days, 
    s.carrier_to_customer_days, s.carrier_delivery_delay_days, s.customer_delivery_delay_days
FROM gold.stream.fact_order_lead_days s
WHERE s.delivered_customer IS NULL