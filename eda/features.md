# Olist 배송 지연 예측 피처 목록

> 목적: 배송 지연 예측 모델링을 위한 Feature Engineering 설계  
> 분류 기준: 예측 기여 요인 중심 의미 기반 카테고리

## 타겟 변수

| Feature Name       | 계산 로직                                                                 | 설명                       |
|--------------------|----------------------------------------------------------------------------|----------------------------|
| `is_delivered_late` | `order_delivered_customer_date > order_estimated_delivery_date`           | 배송 지연 여부 (분류 타겟) |
| `late_by_days`      | `(actual_delivery_days - estimated_delivery_days).clip(lower=0)`          | 배송 지연 일수 (회귀 타겟) |

## 주문 구조 피처

| Feature Name           | 계산 로직                                               | 설명                         |
|------------------------|----------------------------------------------------------|------------------------------|
| `order_items_count`    | `groupby order_id: count()`                              | 주문당 상품 수               |
| `has_multiple_sellers` | `groupby order_id: seller_id.nunique() > 1`             | 주문에 여러 셀러 포함 여부   |

## 시간 및 계절성 정보

| Feature Name              | 계산 로직                                                             | 설명                               |
|---------------------------|------------------------------------------------------------------------|------------------------------------|
| `order_hour`              | `order_purchase_timestamp.dt.hour`                                    | 주문 시간                          |
| `order_day_of_week`       | `order_purchase_timestamp.dt.dayofweek`                               | 주문 요일 (0=월요일)               |
| `order_month`             | `order_purchase_timestamp.dt.month`                                   | 주문 월                            |
| `order_week_of_year`      | `order_purchase_timestamp.dt.isocalendar().week`                      | 연중 주차                          |
| `is_weekend_order`        | `order_day_of_week.isin([5, 6])`                                      | 주말 주문 여부                     |
| `is_holiday_season`       | `order_purchase_timestamp.dt.month.isin([11, 12])`                    | 연말 성수기 여부                   |
| `days_to_shipping_limit`  | `shipping_limit_date - order_purchase_timestamp`                      | 출고 마감까지 여유 일수           |
| `is_same_day_shipping`    | `days_to_shipping_limit <= 1`                                         | 당일/익일 출고 여부               |

## 배송 지연 관련 통계 피처 (과거 기준 집계)

| Feature Name              | 계산 로직                                                      | 설명                               |
|---------------------------|------------------------------------------------------------------|------------------------------------|
| `actual_delivery_days`    | `order_delivered_customer_date - order_purchase_timestamp`       | 실제 배송 소요 일수                |
| `estimated_delivery_days` | `order_estimated_delivery_date - order_purchase_timestamp`       | 예상 배송 소요 일수                |
| `state_avg_delay_days`    | `groupby customer_state: 평균 지연 일수`                         | 주별 평균 지연 일수 (리키지 주의) |
| `seller_avg_delay_days`   | `groupby seller_id: 평균 지연 일수`                              | 셀러별 평균 지연 일수 (리키지 주의) |
| `category_avg_delay_days` | `groupby product_category_name: 평균 지연 일수`                  | 카테고리별 평균 지연 일수          |

## 거리 및 지역 정보

| Feature Name                 | 계산 로직                                                   | 설명                         |
|------------------------------|--------------------------------------------------------------|------------------------------|
| `customer_state`            | `customers.customer_state`                                  | 고객 위치 주                 |
| `seller_state`              | `sellers.seller_state`                                      | 셀러 위치 주                 |
| `customer_seller_distance_km` | `haversine(customer_lat, customer_lng, seller_lat, seller_lng)` | 고객-셀러 간 직선 거리       |
| `is_same_state_shipping`    | `customer_state == seller_state`                            | 고객-셀러 동일 주 여부       |

## 결제 정보

| Feature Name            | 계산 로직                                    | 설명          |
|-------------------------|-----------------------------------------------|---------------|
| `payment_type`          | `payments.payment_type`                      | 결제 수단     |
| `payment_installments`  | `payments.payment_installments`              | 할부 개월 수  |
| `payment_value_total`   | `groupby order_id: payment_value.sum()`      | 총 결제 금액  |

## 셀러 성과 및 제품 특성

| Feature Name              | 계산 로직                                                  | 설명                         |
|---------------------------|-------------------------------------------------------------|------------------------------|
| `seller_delay_rate`       | `groupby seller_id: mean(is_delivered_late)`                | 셀러별 지연률                |
| `seller_order_count`      | `groupby seller_id: count()`                                | 셀러별 총 주문 수            |
| `seller_category_count`   | `groupby seller_id: product_category_name.nunique()`        | 셀러별 카테고리 다양성       |
| `product_category`        | `products.product_category_name`                            | 상품 카테고리                |
| `product_volume_cm3`      | `length * height * width`                                   | 상품 부피 (cm³)              |
| `product_weight_g`        | `products.product_weight_g`                                 | 상품 무게 (g)                |

## 고객 이력 및 행동

| Feature Name             | 계산 로직                                  | 설명                    |
|--------------------------|---------------------------------------------|-------------------------|
| `customer_order_count`   | `groupby customer_id: count()`              | 고객 총 주문 수         |
| `is_repeat_customer`     | `customer_order_count > 1`                  | 재구매 고객 여부        |

## 리뷰 기반 감성 피처 (리키지 여부 검토 필요)

| Feature Name           | 계산 로직                                           | 설명                         |
|------------------------|------------------------------------------------------|------------------------------|
| `customer_avg_rating`  | `groupby customer_id: review_score.mean()`           | 고객 평균 평점               |
| `product_avg_rating`   | `groupby product_id: review_score.mean()`            | 상품 평균 평점               |

## 비용 관련 피처

| Feature Name         | 계산 로직                             | 설명                     |
|----------------------|----------------------------------------|--------------------------|
| `freight_ratio`      | `freight_value / price`                | 운송비 / 상품 가격 비율  |
| `order_total_value`  | `groupby order_id: price.sum()`        | 주문 총 상품 금액        |