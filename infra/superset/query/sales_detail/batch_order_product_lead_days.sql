WITH ProductGroupByPeriod AS (
    SELECT
        product_id,
        category,
        group,
        sales_period
    FROM gold.batch.fact_monthly_sales_by_product
),

LeadDaysWithSalesPeriod AS (
    SELECT
        order_id,
        date_format(delivered_customer, 'yyyy-MM') AS sales_period,
        purchase_to_approval_days,
        approval_to_carrier_days,
        carrier_to_customer_days,
        carrier_delivery_delay_days,
        customer_delivery_delay_days
    FROM gold.batch.fact_order_lead_days
    WHERE delivered_customer IS NOT NULL
),

OrderProduct AS (
    SELECT
        order_id,
        product_id
    FROM gold.batch.fact_order_detail
)

SELECT *
FROM OrderProduct
JOIN LeadDaysWithSalesPeriod
    USING(order_id)
JOIN ProductGroupByPeriod
    USING(product_id, sales_period);