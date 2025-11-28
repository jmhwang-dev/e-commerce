with LatestProductPorfolioGroup as (
  select
  product_id,
  group,
  rank() over(partition by product_id order by sales_period desc) as rnk
  from gold.batch.fact_monthly_sales_by_product
)

select
od.product_id,
od.order_id,
od.customer_id,
od.seller_id,
od.category,
od.quantity,
od.unit_price,
lg.group as latest_group
from gold.batch.fact_order_detail od
left join (
select *
from LatestProductPorfolioGroup
where rnk = 1) lg
using(product_id)