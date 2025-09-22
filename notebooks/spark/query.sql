WITH ProductStats AS (
    -- 분석할 제품 데이터가 있는 테이블 (Silver 또는 다른 테이블)
    SELECT
        product_id,
        COUNT(order_id) AS order_count,
        AVG(price) AS avg_price,
        SUM(price) AS total_selling_price
    FROM
        your_sales_table -- 실제 테이블명으로 변경 필요
    GROUP BY
        product_id
),
Thresholds AS (
    -- Window 함수를 이용해 전체 데이터에 대한 기준점(Threshold) 계산
    SELECT
        *,
        PERCENTILE_CONT(0.75) OVER() WITHIN GROUP (ORDER BY order_count) AS order_count_threshold,
        PERCENTILE_CONT(0.5) OVER() WITHIN GROUP (ORDER BY avg_price) AS median_avg_price
    FROM
        ProductStats
)
-- 최종적으로 CASE 문을 이용해 그룹 분류
SELECT
    product_id,
    order_count,
    avg_price,
    total_selling_price,
    CASE
        WHEN order_count >= order_count_threshold AND avg_price >= median_avg_price THEN 'Star Products'
        WHEN order_count >= order_count_threshold AND avg_price < median_avg_price THEN 'Volume Drivers'
        WHEN order_count < order_count_threshold AND avg_price >= median_avg_price THEN 'Niche Gems'
        ELSE 'Question Marks'
    END AS "group" -- Superset에서 컬럼명으로 인식되도록 alias 설정
FROM
    Thresholds