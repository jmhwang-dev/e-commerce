# EDA

## 고객경험이 안좋다는 것에 대한 판단
- 참조 데이터: `olist_order_reviews_dataset.csv`

    - 낮은 리뷰 점수: `review_score`
    - 부정적인 리뷰 내용:`review_comment_message`

## 고객경험이 안좋은 이유에 대한 가설

1. 예상 배송 날짜 보다 배송완료 날짜가 늦음: `order_estimated_delivery_date` > `order_delivered_customer_date`
    
    - 판매자 귀책
        - 판매자가 배송사에 기한 내 보내지 못함: `shipping_limit_date` > `order_delivered_carrier_date`
            - 주문량 폭주로 판매자 업무 과중: 특정 날짜에 `order_id` 개수

        - 판매자가 배송사에 물건을 잘 못 보내서 다시 재배송
            - 주문량 폭주로 창고에서 혼선 -> 다른 물건 패키징
            - 비슷한 물건으로 오인하여 배송

        - [Mutual fault] 구매자의 늦은 주문 취소로 물품 배송

    - 배송처 귀책
        - 늦은 배송: `order_estimated_delivery_date` > `order_delivered_customer_date`
            - 자연재해: 
            - 배송 중 물건을 분실
            - 주문 주소 착오로 오배송

    - 구매자 귀책
        - 주문 실수
            - 주소지 오류
            
        - [Mutual fault] 구매자의 늦은 주문 취소
            - 더 저렴한 가격 발견
            - 필요해지지 않음

2. 구매 물품이 다름
    - 판매자 귀책
        - 상품 설명 미흡으로 구매자가 원하던 물품이 아님
        - 상품 설명이 잘못되어 구매자가 원하던 물품이 아님
            - 크기: `product_length_cm`, `product_height_cm`, `product_width_cm`
            - 무게: `product_weight_g`
            - 이름: `product_category_name`

    - 구매자 귀책
        - 주문 실수
            - 상품 설명을 제대로 확인 안함
            - 옵션 기재 실수
                - 크기: `product_length_cm`, `product_height_cm`, `product_width_cm`
                - 무게: `product_weight_g`
                - 이름: `product_category_name`
                - 수량
                - 색상

3. 결제 과정 관련된 이유
    - 모종의 이유로 계속된 결제 실패: `payment_sequential`