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
