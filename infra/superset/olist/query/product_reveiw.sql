with ReviewLeadDays as (
select 
order_id,
review_id,
review_score,
date_format(review_creation_date, 'yyyy-MM') AS review_period,
until_answer_lead_days
from gold.batch.fact_review_answer_lead_days
),

ProductReview as (
select *
from ReviewLeadDays
join (
  select order_id, product_id
  from gold.batch.fact_order_detail
) using(order_id)
join (
  select product_id, group, sales_period
  from gold.batch.fact_monthly_sales_by_product
)
using(product_id)
where review_period = sales_period
)

select * from ProductReview
order by product_id