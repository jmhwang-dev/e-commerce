# Olist 배송 지연 예측을 위한 정리된 피처 목록

> 목적: 배송 지연 여부 및 지연 일수 예측 모델 구축을 위한 주요 피처 정리  
> 기준: 구현 가능성, 데이터 출처, 실용성, 해석력

## 타겟 변수

| Feature Name        | 계산 로직                                                                 | Description                  |
|---------------------|--------------------------------------------------------------------------|------------------------------|
| `is_delivered_late` | `order_delivered_customer_date > order_estimated_delivery_date`          | 배송 지연 여부 (분류 타겟)   |
| `late_by_days`      | `(actual_delivery_days - estimated_delivery_days).clip(lower=0)`         | 배송 지연 일수 (회귀 타겟)   |

## 물리적 거리 관련 피처

| Feature Name                   | Data Source                    | 계산 로직                                                             | Description            |
|--------------------------------|---------------------------------|------------------------------------------------------------------------|------------------------|
| `customer_state`              | `customers`                     | `customers.customer_state`                                            | 고객 소재 주           |
| `seller_state`                | `sellers`                       | `sellers.seller_state`                                                | 셀러 소재 주           |
| `customer_seller_distance_km`| `customers`, `sellers`, `geolocation` | `haversine(customer_lat, customer_lng, seller_lat, seller_lng)` | 고객-셀러 간 직선 거리 |
| `is_same_state_shipping`     | `customers`, `sellers`          | `customer_state == seller_state`                                      | 고객-셀러 동일 주 여부 |

## 시간 관련 피처

| Feature Name              | Data Source              | 계산 로직                                                              | Description              |
|---------------------------|---------------------------|-------------------------------------------------------------------------|--------------------------|
| `actual_delivery_days`    | `orders`                  | `(order_delivered_customer_date - order_purchase_timestamp).dt.days`   | 실제 배송 소요 일수     |
| `estimated_delivery_days` | `orders`                  | `(order_estimated_delivery_date - order_purchase_timestamp).dt.days`   | 예상 배송 일수          |
| `days_to_shipping_limit`  | `orders`                  | `(shipping_limit_date - order_purchase_timestamp).dt.days`             | 출고 마감까지 여유 일수 |
| `is_holiday_season`       | `orders`                  | `order_purchase_timestamp.dt.month.isin([11, 12])`                     | 연말 시즌 여부          |
| `order_hour`              | `orders`                  | `order_purchase_timestamp.dt.hour`                                     | 주문 시간               |
| `order_day_of_week`       | `orders`                  | `order_purchase_timestamp.dt.dayofweek`                                | 주문 요일 (0=월요일)    |
| `order_month`             | `orders`                  | `order_purchase_timestamp.dt.month`                                    | 주문 월                 |
| `order_week_of_year`      | `orders`                  | `order_purchase_timestamp.dt.isocalendar().week`                       | 연중 주차               |
| `is_same_day_shipping`    | `orders`                  | `days_to_shipping_limit <= 1`                                          | 당일/익일 출고 여부     |
| `is_weekend_order`        | `orders`                  | `order_day_of_week.isin([5, 6])`                                       | 주말 주문 여부          |
| `state_avg_delay_days`    | `orders`, `customers`     | `groupby customer_state: 평균 지연 일수`                               | 주별 평균 배송 지연     |
| `seller_avg_delay_days`   | `orders`, `order_items`   | `groupby seller_id: 평균 지연 일수`                                    | 셀러별 평균 배송 지연   |

## 비용 관련 피처

| Feature Name           | Data Source     | 계산 로직                                      | Description             |
|------------------------|------------------|-------------------------------------------------|-------------------------|
| `freight_ratio`        | `order_items`     | `freight_value / price`                         | 상품 대비 운송비 비율   |
| `order_total_value`    | `order_items`     | `groupby order_id: price.sum()`                 | 주문 총 금액            |

## 셀러 관련 피처

| Feature Name            | Data Source             | 계산 로직                                               | Description            |
|-------------------------|--------------------------|----------------------------------------------------------|------------------------|
| `seller_delay_rate`     | `orders`, `order_items`  | `groupby seller_id: mean(is_delivered_late)`             | 셀러별 지연률         |
| `product_category`      | `products`               | `product_category_name`                                  | 상품 카테고리         |
| `product_volume_cm3`    | `products`               | `length * height * width`                                | 상품 부피 (cm³)        |
| `product_weight_g`      | `products`               | `product_weight_g`                                       | 상품 무게 (g)          |

## 고객 관련 피처

| Feature Name               | Data Source           | 계산 로직                                                | Description             |
|----------------------------|------------------------|-----------------------------------------------------------|-------------------------|
| `customer_order_count`     | `orders`               | `groupby customer_id: count()`                            | 고객 총 주문 수         |
| `is_repeat_customer`       | `orders`               | `customer_order_count > 1`                                | 재구매 고객 여부        |
| `category_avg_delay_days`  | `orders`, `products`   | `groupby product_category_name: 평균 지연 일수`           | 카테고리별 평균 지연    |

## 주문 관련 피처

| Feature Name            | Data Source               | 계산 로직                                                | Description             |
|-------------------------|----------------------------|-----------------------------------------------------------|-------------------------|
| `has_multiple_sellers`  | `order_items`              | `groupby order_id: seller_id.nunique() > 1`              | 다중 셀러 주문 여부     |
| `order_items_count`     | `order_items`              | `groupby order_id: count()`                              | 주문당 아이템 수        |
| `seller_category_count` | `order_items`, `products`  | `groupby seller_id: product_category_name.nunique()`     | 셀러별 취급 카테고리 수 |
| `seller_order_count`    | `order_items`              | `groupby seller_id: count()`                             | 셀러별 주문 수          |

## 결제 관련 피처

| Feature Name            | Data Source  | 계산 로직                                        | Description        |
|-------------------------|---------------|---------------------------------------------------|--------------------|
| `payment_installments`  | `payments`    | `payments.payment_installments`                  | 할부 개월 수       |
| `payment_type`          | `payments`    | `payments.payment_type`                          | 결제 수단          |
| `payment_value_total`   | `payments`    | `groupby order_id: payment_value.sum()`          | 총 결제 금액       |

## 평점 관련 피처

| Feature Name           | Data Source              | 계산 로직                                     | Description       |
|------------------------|---------------------------|------------------------------------------------|-------------------|
| `customer_avg_rating`  | `reviews`                 | `groupby customer_id: review_score.mean()`     | 고객별 평균 평점  |
| `product_avg_rating`   | `reviews`, `order_items`  | `groupby product_id: review_score.mean()`      | 상품별 평균 평점  |