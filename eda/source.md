## 📅 날짜 기반

| 피처 이름              | 설명                    | 선정 이유                                  | 원본 테이블   | 원본 컬럼                                       |
|:-----------------------|:------------------------|:-------------------------------------------|:--------------|:------------------------------------------------|
| `days_to_limit`        | 출고 마감까지 남은 시간 | 셀러 준비 시간의 여유 판단 기준            | `orders`      | `shipping_limit_date, order_purchase_timestamp` |
| `is_year_end_sale`     | 11~12월 주문 여부       | 시즌성 물량 증가 구간                      | `orders`      | `order_purchase_timestamp`                      |
| `order_before_weekend` | 주문일이 금/토인지 여부 | 주말 진입 전 주문이면 지연 가능성 있음     | `orders`      | `order_purchase_timestamp`                      |
| `order_day_of_week`    | 주문 요일               | 주말 주문은 처리/배송이 지연될 가능성 있음 | `orders`      | `order_purchase_timestamp`                      |
| `order_hour`           | 주문 시간               | 야간 주문은 처리 지연 가능성 존재          | `orders`      | `order_purchase_timestamp`                      |
| `order_month`          | 주문 월                 | 시즌성 및 휴가 시즌 영향 반영              | `orders`      | `order_purchase_timestamp`                      |
| `order_weekofyear`     | 연중 몇 번째 주인지     | 시즌별 트렌드 반영 가능                    | `orders`      | `order_purchase_timestamp`                      |

---

## 📍 위치 기반

| 피처 이름         | 설명                        | 선정 이유             | 원본 테이블          | 원본 컬럼                                                                   |
|:------------------|:----------------------------|:----------------------|:---------------------|:----------------------------------------------------------------------------|
| `avg_state_delay` | state별 평균 배송 지연 일수 | 지역별 물류 성과 반영 | `orders + customers` | `customer_id, order_delivered_customer_date, order_estimated_delivery_date` |

---

## 📦 주문 및 배송

| 피처 이름                                                  | 설명                                        | 선정 이유                                   | 원본 테이블                    | 원본 컬럼                                                      |
|:-----------------------------------------------------------|:--------------------------------------------|:--------------------------------------------|:-------------------------------|:---------------------------------------------------------------|
| `delivered_late`                                           | 실제 배송일 > 예상일 여부 (지연 여부)       | 이진 분류 타겟으로 사용 가능                | `orders`                       | `order_delivered_customer_date, order_estimated_delivery_date` |
| `freight_ratio`                                            | 운송비 비율 (`freight_value / total_price`) | 거리에 따른 비용 비율로 간접 거리 반영 가능 | `order_items`                  | `freight_value, price`                                         |
| `multiple_sellers_in_order`                                | 주문에 여러 셀러가 포함되었는지 여부        | 병합 배송일 경우 지연 가능성 증가           | `order_items`                  | `order_id, seller_id`                                          |
| `order_delivered_customer_date - order_purchase_timestamp` | 실제 배송 소요 시간                         | 타겟 변수 정의 및 성능 비교에 사용          | `orders`                       | `order_delivered_customer_date, order_purchase_timestamp`      |
| `order_estimated_delivery_date - order_purchase_timestamp` | 예상 배송 소요 시간                         | 모델의 기준이 되는 시스템 기대값            | `orders`                       | `order_estimated_delivery_date, order_purchase_timestamp`      |
| `order_items_count`                                        | 주문에 포함된 상품 수                       | 아이템 수가 많을수록 묶음배송 지연 가능성   | `order_items`                  | `order_id`                                                     |
| `order_price_total`                                        | 주문 총 금액 (`sum(price)`)                 | 고가 주문은 특별 취급 가능성                | `order_items`                  | `order_id, price`                                              |
| `same_day_shipping`                                        | 출고 마감까지 1일 이하 여부                 | 긴급 배송은 배송 리스크 증가 가능성         | `orders`                       | `shipping_limit_date, order_purchase_timestamp`                |
| `same_state_shipping`                                      | 고객과 셀러의 state 일치 여부               | 장거리 여부에 따라 배송 지연 위험이 달라짐  | `orders + sellers + customers` | `customer_id, seller_id`                                       |
| `shipping_limit_date - order_purchase_timestamp`           | 판매자의 출고 마감까지 여유 시간            | 배송 지연 위험 예측에 직접적으로 관련       | `orders`                       | `shipping_limit_date, order_purchase_timestamp`                |

---

## 🏬 판매자 및 상품 정보

| 피처 이름                             | 설명                                       | 선정 이유                                           | 원본 테이블              | 원본 컬럼    |
|:--------------------------------------|:-------------------------------------------|:----------------------------------------------------|:-------------------------|:-------------|
| `average_review_message_length`       | 평균 리뷰 메시지 길이                      | 긴 리뷰는 감정 개입이 큰 경우 많음                  | `(unknown)`              | ``           |
| `avg_delay_by_product_category`       | 카테고리별 평균 배송 지연 일수             | 상품군에 따라 배송 특성이 다를 수 있음              | `(unknown)`              | ``           |
| `category_3m_return_rate`             | 카테고리별 최근 3개월 반품 비율            | 제품군 신뢰도 추정                                  | `(unknown)`              | ``           |
| `category_delay_ratio`                | 해당 카테고리의 배송 지연률                | 제품군의 상대적 리스크 평가                         | `(unknown)`              | ``           |
| `category_weighted_delay`             | 카테고리별 평균 지연일 × 판매 비중         | 상품군 기반 전체 배송 리스크 반영                   | `(unknown)`              | ``           |
| `cumulative_order_count_till_now`     | 주문 시점까지의 누적 주문 수               | 플랫폼 전반의 수요 피크 분석                        | `(unknown)`              | ``           |
| `customer_avg_order_gap_days`         | 주문 간 평균 일수                          | 구매 주기 및 고객 유형 파악 가능                    | `(unknown)`              | ``           |
| `customer_avg_review_score`           | 고객이 남긴 리뷰 점수 평균                 | 고객의 평가 성향 파악 (까다로운 고객 여부)          | `(unknown)`              | ``           |
| `customer_avg_waiting_days`           | 고객의 과거 평균 배송 대기일               | 기대치 대비 불만 가능성 예측                        | `(unknown)`              | ``           |
| `customer_city`                       | 고객의 도시 정보                           | 도시별 물류 인프라 차이를 고려 가능                 | `(unknown)`              | ``           |
| `customer_cluster_id`                 | 고객 클러스터 ID                           | 행동 기반 유사 고객 구분                            | `(unknown)`              | ``           |
| `customer_complaint_history`          | 이전 불만 제기 여부                        | 이전 불만 고객은 예민할 가능성                      | `(unknown)`              | ``           |
| `customer_delay_experience_ratio`     | 고객이 과거에 배송 지연을 경험한 비율      | 고객 불만 및 재구매율과의 연결성 분석               | `(unknown)`              | ``           |
| `customer_order_count`                | 고객의 누적 주문 횟수                      | 충성 고객 여부, 행동 패턴 모델링 가능               | `(unknown)`              | ``           |
| `customer_order_frequency`            | 월간 평균 주문 빈도                        | 충성 고객일수록 기대 수준이 높음                    | `(unknown)`              | ``           |
| `customer_recency`                    | 고객의 마지막 주문 이후 일수               | 최근 활동 고객은 이탈 민감도가 높음                 | `(unknown)`              | ``           |
| `customer_region_avg_delay`           | 고객 거주 지역의 평균 지연률               | 외부적 배송 품질 파악 가능                          | `(unknown)`              | ``           |
| `customer_return_ratio`               | 저평가 리뷰 비율 (1~2점 / 전체)            | 고객 만족도 지표                                    | `(unknown)`              | ``           |
| `customer_returning_rate`             | 고객의 재구매율                            | 이탈 가능성과 불만 간 관계 파악                     | `(unknown)`              | ``           |
| `customer_state`                      | 고객의 주                                  | 지역별 물류 인프라 차이 반영 가능                   | `(unknown)`              | ``           |
| `days_since_last_order`               | 이전 주문과의 일수 차이                    | 재구매 주기에 따른 행동 패턴 분석                   | `(unknown)`              | ``           |
| `days_until_holiday`                  | 다음 공휴일까지 남은 일수                  | 공휴일 영향권 여부 (외부데이터 필요)                | `(unknown)`              | ``           |
| `delivery_delay_count_by_seller`      | 셀러의 배송 지연 횟수                      | 과거 이력 기반 위험도 추정                          | `(unknown)`              | ``           |
| `distance_lead_time_product`          | 구매자-셀러 거리 × 리드타임                | 멀리 배송되며 시간이 오래 걸리는 패턴 파악          | `(unknown)`              | ``           |
| `federal_holiday_flag`                | 출고일이 브라질 공휴일 여부                | 공휴일 배송 가능성 제한                             | `(unknown)`              | ``           |
| `first_order`                         | 해당 고객의 첫 주문 여부                   | 첫 경험은 리뷰/불만 영향이 큼                       | `(unknown)`              | ``           |
| `freight_cost_per_item`               | 운송비 / 주문 아이템 수                    | 아이템 수 대비 배송비 산정이 높은 경우 문제 가능성  | `(unknown)`              | ``           |
| `is_holiday_shipping_window`          | 배송기간 내 공휴일 포함 여부               | 공휴일은 배송 지연 가능성 상승                      | `(unknown)`              | ``           |
| `is_weekend_delivery`                 | 배송일이 주말인지 여부                     | 주말 배송 성공률은 낮을 수 있음                     | `(unknown)`              | ``           |
| `is_weekend_order`                    | 주말 주문 여부                             | 주말은 물류 흐름 지연 가능성 상승                   | `(unknown)`              | ``           |
| `item_count_per_order`                | 주문당 상품 수량                           | 묶음 배송이 걸리면 지연될 수 있음                   | `(unknown)`              | ``           |
| `late_by_days`                        | 실제 배송일 - 예상 배송일                  | 지연 정도를 수치화해 타겟 회귀 모델에도 활용 가능   | `(unknown)`              | ``           |
| `num_products_same_category`          | 동일 카테고리의 상품 수                    | 상품군 통일성은 포장 및 물류 효율에 기여 가능       | `(unknown)`              | ``           |
| `order_city_distance_bin`             | 구매자-셀러 거리 구간화 (가까움/중간/멀리) | 거리 기반 범주형 변수 처리                          | `(unknown)`              | ``           |
| `order_item_price_ratio`              | 단일 아이템 가격 / 주문 총액               | 고가 단품 여부 판단, 배송 민감도 추정               | `(unknown)`              | ``           |
| `order_item_total_price`              | 제품 수 * 단가 (개별 총액)                 | 고가 상품은 취급주의 대상일 수 있음                 | `(unknown)`              | ``           |
| `order_month_volume_zscore`           | 해당 월 주문량의 z-score                   | 비정상 수요 탐지                                    | `(unknown)`              | ``           |
| `order_to_shipment_days`              | 주문 후 출고까지 소요 일수                 | 출고 지연 패턴 분석                                 | `(unknown)`              | ``           |
| `payment_installments`                | 할부 개월 수                               | 할부 시 고객 성향/리스크 가능성                     | `(unknown)`              | ``           |
| `payment_type`                        | 결제 방식 (신용카드/볼레토 등)             | 볼레토는 승인 지연 가능성                           | `(unknown)`              | ``           |
| `product_avg_review_score`            | 상품 평균 리뷰 점수                        | 품질에 따른 배송 경험 반영 가능                     | `(unknown)`              | ``           |
| `product_category_delivery_avg_delay` | 상품 카테고리별 평균 배송 지연일           | 제품 특성 기반 예측 정확도 향상                     | `(unknown)`              | ``           |
| `product_category_name`               | 상품 카테고리                              | 무거운 제품은 배송 지연 가능성 높음                 | `order_items + products` | `product_id` |
| `product_is_fragile`                  | 파손 우려 상품 여부 (수작업 태깅)          | 조심스러운 배송은 지연 가능성 높음                  | `(unknown)`              | ``           |
| `product_return_rate`                 | 해당 제품의 반품 비율                      | 문제 소지가 높은 제품 여부 간접 판단                | `(unknown)`              | ``           |
| `product_shipping_delay_ratio`        | 상품의 평균 배송 지연율                    | 특정 상품의 특성상 반복되는 이슈 여부               | `(unknown)`              | ``           |
| `product_topic_from_reviews`          | 리뷰 기반 LDA 제품 토픽                    | 리뷰 주제 기반 제품 특성 추정                       | `(unknown)`              | ``           |
| `product_volume_cm3`                  | 상품 부피 (가로*세로*높이)                 | 부피 큰 제품은 물류 지연 가능성 높음                | `(unknown)`              | ``           |
| `product_weight_g`                    | 상품 무게                                  | 무거운 상품은 배송 처리 지연 가능성                 | `(unknown)`              | ``           |
| `product_weight_volume`               | 제품의 부피 또는 무게 (상품 데이터 필요)   | 배송 난이도, 운송 시간과 상관 가능성                | `(unknown)`              | ``           |
| `repeat_customer`                     | 해당 고객이 재구매 고객인지 여부           | 재구매 고객은 배송 경험에 민감할 수 있음            | `(unknown)`              | ``           |
| `review_emoji_density`                | 이모지 수 / 총 단어 수                     | 강한 감정 표현 정량화                               | `(unknown)`              | ``           |
| `review_length`                       | 리뷰 텍스트의 길이                         | 짧은 리뷰는 강한 감정을 담을 가능성 높음            | `(unknown)`              | ``           |
| `review_sentiment_score`              | 리뷰 감성 점수 (사전 기반)                 | 감정 경향성과 배송 경험의 상관 추정                 | `(unknown)`              | ``           |
| `road_strike_indicator`               | 브라질 도로 파업 여부 (출고일 기준)        | 파업은 강력한 배송 지연 요인                        | `(unknown)`              | ``           |
| `same_state_flag`                     | 판매자와 고객의 주(state) 동일 여부        | 지역 물류 네트워크 반영                             | `(unknown)`              | ``           |
| `seller_active_days`                  | 셀러의 운영 기간                           | 오래된 셀러일수록 경험이 많아 배송 안정성 기대 가능 | `(unknown)`              | ``           |
| `seller_active_duration`              | 셀러의 첫 활동일 이후 경과일               | 신규/경험 셀러 구분에 활용                          | `(unknown)`              | ``           |
| `seller_avg_review_score`             | 해당 셀러의 평균 리뷰 점수                 | 셀러의 평판 및 신뢰도 지표                          | `(unknown)`              | ``           |
| `seller_avg_shipping_delay`           | 셀러 평균 지연 일수                        | 셀러 성과의 신뢰도 지표                             | `(unknown)`              | ``           |
| `seller_behavior_embedding`           | 셀러 행동 임베딩 벡터                      | 셀러 특성 고차원 표현                               | `(unknown)`              | ``           |
| `seller_cancel_rate`                  | 셀러 주문 취소 비율                        | 셀러 운영 안정성 지표                               | `(unknown)`              | ``           |
| `seller_cancellation_rate`            | 셀러의 주문 취소 비율                      | 셀러 신뢰도 평가                                    | `(unknown)`              | ``           |
| `seller_category_diversity`           | 셀러가 다루는 카테고리 수                  | 품목 다양성은 복잡도 증가 요인                      | `(unknown)`              | ``           |
| `seller_city`                         | 판매자의 도시 정보                         | 판매자 위치 기반 성과 분석                          | `(unknown)`              | ``           |
| `seller_delay_rate`                   | 셀러 지연 배송 비율                        | 셀러의 물류 신뢰도 판단                             | `(unknown)`              | ``           |
| `seller_delay_ratio`                  | 셀러의 평균 배송 지연률                    | 과거 이력 기반 위험도 예측 가능                     | `(unknown)`              | ``           |
| `seller_delay_trend_3m`               | 최근 3개월간 배송 지연 추세                | 배송 서비스 개선 또는 악화 탐지                     | `(unknown)`              | ``           |
| `seller_id`                           | 판매자 ID                                  | 셀러 품질 통제 및 반복 패턴 분석 가능               | `order_items`            | `seller_id`  |
| `seller_is_multi_category`            | 판매자가 다양한 카테고리를 판매하는지 여부 | 업무 복잡성이 배송 효율에 영향 가능                 | `(unknown)`              | ``           |
| `seller_item_count`                   | 셀러의 총 판매 아이템 수                   | 물류 처리량 추정 가능                               | `(unknown)`              | ``           |
| `seller_lead_time_stability`          | 셀러의 배송 리드타임 표준편차              | 배송 안정성 판단 지표                               | `(unknown)`              | ``           |
| `seller_order_share`                  | 전체 주문 중 해당 셀러의 비중              | 상위 셀러 여부 및 지연 집중도 추정                  | `(unknown)`              | ``           |
| `seller_region_avg_delay`             | 해당 지역 판매자의 평균 지연율             | 지역 물류 품질 반영 가능                            | `(unknown)`              | ``           |
| `seller_review_score_avg`             | 셀러의 평균 리뷰 점수                      | 셀러의 품질/신뢰도 판단                             | `(unknown)`              | ``           |
| `seller_state`                        | 판매자 주(state)                           | 판매자 지역 기반 물류 여건 반영                     | `(unknown)`              | ``           |
| `seller_total_orders`                 | 해당 셀러의 총 주문 수                     | 셀러의 물류 운영 경험 및 규모 판단                  | `(unknown)`              | ``           |
| `shipping_date_to_estimate_gap`       | 실제 배송일과 예상 배송일의 차이           | 배송 지연을 직접 수치화한 타깃 검증 및 피처 활용    | `(unknown)`              | ``           |
| `shipping_time_of_day`                | 배송 예정 시간대 (오전/오후/야간)          | 배송 시간대는 지연 가능성과 만족도에 영향           | `(unknown)`              | ``           |
| `shipping_to_review_days`             | 배송일과 리뷰 생성일 간 차이               | 배송 경험과 리뷰 간 시차 정보                       | `(unknown)`              | ``           |
| `time_to_event_ratio`                 | 배송예정일까지 남은 시간 / 평균 리드타임   | 긴급성 또는 타임 버퍼로 활용                        | `(unknown)`              | ``           |
| `was_product_exchanged`               | 제품 교환 여부                             | 불만족 간접 지표                                    | `(unknown)`              | ``           |
| `weather_on_shipping_day`             | 출고일 기준 브라질 날씨 정보               | 기상 이슈가 배송에 미치는 영향 분석                 | `(unknown)`              | ``           |