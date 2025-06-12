# 📦 Olist 배송 지연 예측을 위한 정리된 피처 목록

## ✅ 실제 구현 가능한 핵심 피처들

### 📦 주문/배송 관련

| Feature Name | Data Source | 계산 로직 | Description |
|--------------|-------------|------------|-------------|
| `actual_delivery_days` | orders | `(order_delivered_customer_date - order_purchase_timestamp).dt.days` | 실제 배송 소요 일수 |
| `days_to_shipping_limit` | orders | `(shipping_limit_date - order_purchase_timestamp).dt.days` | 출고 마감까지 여유 일수 |
| `estimated_delivery_days` | orders | `(order_estimated_delivery_date - order_purchase_timestamp).dt.days` | 예상 배송 소요 일수 |
| `freight_ratio` | order_items | `freight_value / price` | 상품 대비 운송비 비율 |
| `has_multiple_sellers` | order_items | `order_items.groupby('order_id')['seller_id'].nunique() > 1` | 주문에 여러 셀러 포함 여부 |
| `is_delivered_late` | orders | `order_delivered_customer_date > order_estimated_delivery_date` | 배송 지연 여부 (타겟 변수) |
| `is_same_day_shipping` | orders | `days_to_shipping_limit <= 1` | 당일/익일 출고 여부 |
| `is_same_state_shipping` | customers, sellers | `customers.customer_state == sellers.seller_state` | 고객-셀러 동일 주 여부 |
| `order_items_count` | order_items | `order_items.groupby('order_id').size()` | 주문당 아이템 수 |
| `order_total_value` | order_items | `order_items.groupby('order_id')['price'].sum()` | 주문 총 금액 |

### 📅 날짜/시간 기반

| Feature Name | Data Source | 계산 로직 | Description |
|--------------|-------------|------------|-------------|
| `is_holiday_season` | orders | `order_purchase_timestamp.dt.month.isin([11, 12])` | 연말 시즌 주문 여부 |
| `is_weekend_order` | orders | `order_purchase_timestamp.dt.dayofweek.isin([5, 6])` | 주말 주문 여부 |
| `order_day_of_week` | orders | `order_purchase_timestamp.dt.dayofweek` | 주문 요일 (0=월요일) |
| `order_hour` | orders | `order_purchase_timestamp.dt.hour` | 주문 시간 |
| `order_month` | orders | `order_purchase_timestamp.dt.month` | 주문 월 |
| `order_week_of_year` | orders | `order_purchase_timestamp.dt.isocalendar().week` | 연중 주차 |

### 📍 위치 기반

| Feature Name | Data Source | 계산 로직 | Description |
|--------------|-------------|------------|-------------|
| `customer_seller_distance_km` | customers, sellers, geolocation | `haversine(customer_lat, customer_lng, seller_lat, seller_lng)` | 고객-셀러 간 직선거리 |
| `customer_state` | customers | `customers.customer_state` | 고객 소재 주 |
| `seller_state` | sellers | `sellers.seller_state` | 판매자 소재 주 |

### 🏬 판매자 관련

| Feature Name | Data Source | 계산 로직 | Description |
|--------------|-------------|------------|-------------|
| `seller_avg_delay_days` | orders, order_items | `groupby seller_id: (actual_delivery_days - estimated_delivery_days).mean()` | 셀러별 평균 지연 일수 |
| `seller_delay_rate` | orders, order_items | `groupby seller_id: (is_delivered_late).mean()` | 셀러별 배송 지연률 |
| `seller_order_count` | order_items | `order_items.groupby('seller_id').size()` | 셀러별 총 주문 수 |
| `seller_category_count` | order_items, products | `groupby seller_id: product_category_name.nunique()` | 셀러가 판매하는 카테고리 수 |

### 👤 고객 관련

| Feature Name | Data Source | 계산 로직 | Description |
|--------------|-------------|------------|-------------|
| `customer_order_count` | orders | `orders.groupby('customer_id').size()` | 고객별 총 주문 횟수 |
| `customer_avg_rating` | reviews | `reviews.groupby('customer_id')['review_score'].mean()` | 고객별 평균 평점 |
| `is_repeat_customer` | orders | `customer_order_count > 1` | 재구매 고객 여부 |

### 🧾 상품 관련

| Feature Name | Data Source | 계산 로직 | Description |
|--------------|-------------|------------|-------------|
| `product_category` | products | `products.product_category_name` | 상품 카테고리 |
| `product_weight_g` | products | `products.product_weight_g` | 상품 무게 |
| `product_volume_cm3` | products | `product_length_cm * product_height_cm * product_width_cm` | 상품 부피 |
| `product_avg_rating` | reviews, order_items | `groupby product_id: review_score.mean()` | 상품별 평균 평점 |

### 💳 결제 관련

| Feature Name | Data Source | 계산 로직 | Description |
|--------------|-------------|------------|-------------|
| `payment_type` | payments | `payments.payment_type` | 결제 수단 |
| `payment_installments` | payments | `payments.payment_installments` | 할부 개월 수 |
| `payment_value_total` | payments | `payments.groupby('order_id')['payment_value'].sum()` | 총 결제 금액 |

### 📊 집계 기반 파생 피처

| Feature Name | Data Source | 계산 로직 | Description |
|--------------|-------------|------------|-------------|
| `category_avg_delay_days` | orders, order_items, products | `groupby product_category_name: (actual_delivery_days - estimated_delivery_days).mean()` | 카테고리별 평균 지연 일수 |
| `state_avg_delay_days` | orders, customers | `groupby customer_state: (actual_delivery_days - estimated_delivery_days).mean()` | 주별 평균 지연 일수 |
| `late_by_days` | orders | `(actual_delivery_days - estimated_delivery_days).clip(lower=0)` | 지연 일수 (회귀 타겟) |

## 🎯 최종 권장 피처 세트 (30개)

**기본 피처 (15개)**
- `actual_delivery_days`, `estimated_delivery_days`, `days_to_shipping_limit`
- `is_delivered_late`, `late_by_days`, `freight_ratio`
- `order_items_count`, `order_total_value`, `has_multiple_sellers`
- `order_day_of_week`, `order_hour`, `order_month`
- `customer_state`, `seller_state`, `product_category`

**파생 피처 (15개)**
- `customer_seller_distance_km`, `is_same_state_shipping`
- `seller_delay_rate`, `seller_order_count`, `seller_category_count`
- `customer_order_count`, `is_repeat_customer`, `customer_avg_rating`
- `product_weight_g`, `product_volume_cm3`, `product_avg_rating`
- `payment_type`, `payment_installments`, `category_avg_delay_days`, `state_avg_delay_days`