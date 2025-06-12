# 📦 Olist 배송 지연 예측을 위한 정리된 피처 목록

## ✅ 실제 구현 가능한 핵심 피처들 (Data Source 기준 정렬)

| Feature Name | Data Source | 계산 로직 | Description |
|--------------|-------------|------------|-------------|
| `customer_state` | customers | `customers.customer_state` | 고객 소재 주 |
| `customer_seller_distance_km` | customers, sellers, geolocation | `haversine(customer_lat, customer_lng, seller_lat, seller_lng)` | 고객-셀러 간 직선거리 |
| `is_same_state_shipping` | customers, sellers | `customers.customer_state == sellers.seller_state` | 고객-셀러 동일 주 여부 |
| `freight_ratio` | order_items | `freight_value / price` | 상품 대비 운송비 비율 |
| `has_multiple_sellers` | order_items | `order_items.groupby('order_id')['seller_id'].nunique() > 1` | 주문에 여러 셀러 포함 여부 |
| `order_items_count` | order_items | `order_items.groupby('order_id').size()` | 주문당 아이템 수 |
| `order_total_value` | order_items | `order_items.groupby('order_id')['price'].sum()` | 주문 총 금액 |
| `seller_category_count` | order_items, products | `groupby seller_id: product_category_name.nunique()` | 셀러가 판매하는 카테고리 수 |
| `seller_order_count` | order_items | `order_items.groupby('seller_id').size()` | 셀러별 총 주문 수 |
| `actual_delivery_days` | orders | `(order_delivered_customer_date - order_purchase_timestamp).dt.days` | 실제 배송 소요 일수 |
| `days_to_shipping_limit` | orders | `(shipping_limit_date - order_purchase_timestamp).dt.days` | 출고 마감까지 여유 일수 |
| `estimated_delivery_days` | orders | `(order_estimated_delivery_date - order_purchase_timestamp).dt.days` | 예상 배송 소요 일수 |
| `is_delivered_late` | orders | `order_delivered_customer_date > order_estimated_delivery_date` | 배송 지연 여부 (타겟 변수) |
| `is_holiday_season` | orders | `order_purchase_timestamp.dt.month.isin([11, 12])` | 연말 시즌 주문 여부 |
| `is_repeat_customer` | orders | `customer_order_count > 1` | 재구매 고객 여부 |
| `is_same_day_shipping` | orders | `days_to_shipping_limit <= 1` | 당일/익일 출고 여부 |
| `is_weekend_order` | orders | `order_purchase_timestamp.dt.dayofweek.isin([5, 6])` | 주말 주문 여부 |
| `late_by_days` | orders | `(actual_delivery_days - estimated_delivery_days).clip(lower=0)` | 지연 일수 (회귀 타겟) |
| `order_day_of_week` | orders | `order_purchase_timestamp.dt.dayofweek` | 주문 요일 (0=월요일) |
| `order_hour` | orders | `order_purchase_timestamp.dt.hour` | 주문 시간 |
| `order_month` | orders | `order_purchase_timestamp.dt.month` | 주문 월 |
| `order_week_of_year` | orders | `order_purchase_timestamp.dt.isocalendar().week` | 연중 주차 |
| `customer_order_count` | orders | `orders.groupby('customer_id').size()` | 고객별 총 주문 횟수 |
| `state_avg_delay_days` | orders, customers | `groupby customer_state: (actual_delivery_days - estimated_delivery_days).mean()` | 주별 평균 지연 일수 |
| `seller_avg_delay_days` | orders, order_items | `groupby seller_id: (actual_delivery_days - estimated_delivery_days).mean()` | 셀러별 평균 지연 일수 |
| `seller_delay_rate` | orders, order_items | `groupby seller_id: (is_delivered_late).mean()` | 셀러별 배송 지연률 |
| `category_avg_delay_days` | orders, order_items, products | `groupby product_category_name: (actual_delivery_days - estimated_delivery_days).mean()` | 카테고리별 평균 지연 일수 |
| `payment_installments` | payments | `payments.payment_installments` | 할부 개월 수 |
| `payment_type` | payments | `payments.payment_type` | 결제 수단 |
| `payment_value_total` | payments | `payments.groupby('order_id')['payment_value'].sum()` | 총 결제 금액 |
| `product_category` | products | `products.product_category_name` | 상품 카테고리 |
| `product_volume_cm3` | products | `product_length_cm * product_height_cm * product_width_cm` | 상품 부피 |
| `product_weight_g` | products | `products.product_weight_g` | 상품 무게 |
| `customer_avg_rating` | reviews | `reviews.groupby('customer_id')['review_score'].mean()` | 고객별 평균 평점 |
| `product_avg_rating` | reviews, order_items | `groupby product_id: review_score.mean()` | 상품별 평균 평점 |
| `seller_state` | sellers | `sellers.seller_state` | 판매자 소재 주 |

---

## ❌ 제거 후보 피처들 (중복/구현 불가)

### 🔄 중복 피처들

| 제거 후보 Feature | 유지할 Feature | 중복 이유 |
|------------------|----------------|-----------|
| `num_items_per_order` | `order_items_count` | 동일한 의미 |
| `items_per_order` | `order_items_count` | 동일한 의미 |
| `has_multiple_items` | `order_items_count > 1` | 파생 가능 |
| `order_price_total` | `order_total_value` | 동일한 의미 |
| `is_order_before_weekend` | `order_day_of_week.isin([4, 5])` | 파생 가능 |
| `seller_total_orders` | `seller_order_count` | 동일한 의미 |
| `seller_total_items` | - | seller_order_count로 대체 가능 |

### 🚫 외부 데이터 필요 (구현 불가)

| Feature Name | 필요한 외부 데이터 | 이유 |
|--------------|-------------------|------|
| `weather_on_shipping_day` | 브라질 기상 데이터 | Olist 데이터에 없음 |
| `is_order_on_holiday` | 브라질 공휴일 달력 | 외부 API 필요 |
| `road_strike_indicator` | 교통/파업 정보 | 실시간 뉴스 데이터 필요 |
| `federal_holiday_flag` | 공휴일 정보 | 외부 달력 데이터 필요 |
| `is_remote_area` | 지역 분류 데이터 | 별도 지역 정보 필요 |

### 📊 복잡한 계산이 필요한 피처들

| Feature Name | 구현 복잡도 | 이유 |
|--------------|-------------|------|
| `customer_avg_order_interval_days` | 높음 | 시계열 정렬 및 차분 계산 필요 |
| `days_since_last_order` | 높음 | 고객별 주문 이력 추적 필요 |
| `seller_lead_time_stability` | 높음 | 표준편차 계산 복잡 |
| `customer_cluster_id` | 매우 높음 | 별도 클러스터링 모델 필요 |
| `seller_behavior_embedding` | 매우 높음 | 임베딩 모델 구축 필요 |

### 🔗 순환 참조 위험 피처들

| Feature Name | 위험 이유 | 대안 |
|--------------|-----------|------|
| `shipping_date_to_estimate_gap` | 타겟과 동일한 정보 | `late_by_days` 사용 |
| `delivery_delay_count_by_seller` | 미래 정보 누출 가능 | 시점 기준 과거 데이터만 사용 |
| `avg_delay_by_product_category` | 타겟 누출 위험 | 시점 기준 집계 필요 |