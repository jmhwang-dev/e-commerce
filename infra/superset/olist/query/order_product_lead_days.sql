with ProductGroupByPeriod as (
select
  product_id,
  category,
  group,
  sales_period
from gold.fact_monthly_sales_by_product
),
LeadDaysWithSalesPeriod as (
select
  order_id,
  date_format(delivered_customer, 'yyyy-MM') AS sales_period,
  purchase_to_approval_days,
  approval_to_carrier_days,
  carrier_to_customer_days
  carrier_delivery_delay_days,
  customer_delivery_delay_days
from gold.fact_order_lead_days
where delivered_customer is not null
),
OrderProduct as (
select order_id, product_id
from fact_order_detail
)

select * from OrderProduct
join LeadDaysWithSalesPeriod
using(order_id)
join ProductGroupByPeriod
using(product_id, sales_period)