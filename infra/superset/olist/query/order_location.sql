with OrderCustomerLocation as (
select order_id, od.customer_id as user_id, lng, lat, 'customer' as user_type from gold.batch.fact_order_detail od join gold.batch.dim_user_location ul on od.customer_id = ul.user_id
),
OrderSellerLocation as (
select order_id, od.seller_id as user_id, lng, lat, 'seller' as user_type from gold.batch.fact_order_detail od join gold.batch.dim_user_location ul on od.seller_id = ul.user_id
)

select * from OrderCustomerLocation
UNION ALL select * from OrderSellerLocation