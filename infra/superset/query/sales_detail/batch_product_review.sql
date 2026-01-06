WITH ReviewLeadDays AS (
    SELECT
        order_id,
        review_id,
        review_score,
        date_format(review_creation_date, 'yyyy-MM') AS review_period,
        until_answer_lead_days
    FROM gold.batch.fact_review_answer_lead_days
),

ProductReview AS (
    SELECT
        *
    FROM ReviewLeadDays
    JOIN (
        SELECT order_id, product_id
        FROM gold.batch.fact_order_detail
    ) AS od
    USING(order_id)
    JOIN (
        SELECT product_id, group, sales_period
        FROM gold.batch.fact_monthly_sales_by_product
    ) AS p
    USING(product_id)
    WHERE review_period = sales_period
)

SELECT *
FROM ProductReview
ORDER BY product_id;