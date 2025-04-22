import pandas as pd

def generate_column_description(description_data, output_file):    
    description_list = []
    for line in description_data.strip().split('\n')[1:]:
        file_name, column_name, description = line.split(',', 2)
        description_list.append([file_name, column_name, description])

    df_description = pd.DataFrame(description_list, columns=['file_name', 'column_name', 'description'])
    df_description.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Done: '{output_file}'")


description_data = """file_name,column_name,description
olist_customers_dataset.csv,customer_id,고객을 식별하기 위한 고유 ID
olist_customers_dataset.csv,customer_unique_id,고객의 고유 식별자(중복되지 않는 ID)
olist_customers_dataset.csv,customer_zip_code_prefix,고객의 우편번호 접두사
olist_customers_dataset.csv,customer_city,고객이 거주하는 도시
olist_customers_dataset.csv,customer_state,고객이 거주하는 주(state)
olist_geolocation_dataset.csv,geolocation_zip_code_prefix,위치 정보에 해당하는 우편번호 접두사
olist_geolocation_dataset.csv,geolocation_lat,위도(latitude)
olist_geolocation_dataset.csv,geolocation_lng,경도(longitude)
olist_geolocation_dataset.csv,geolocation_city,위치 정보에 해당하는 도시
olist_geolocation_dataset.csv,geolocation_state,위치 정보에 해당하는 주(state)
olist_order_items_dataset.csv,order_id,주문을 식별하기 위한 고유 ID
olist_order_items_dataset.csv,order_item_id,주문 항목을 식별하기 위한 고유 ID
olist_order_items_dataset.csv,product_id,상품을 식별하기 위한 고유 ID
olist_order_items_dataset.csv,seller_id,판매자를 식별하기 위한 고유 ID
olist_order_items_dataset.csv,shipping_limit_date,판매자가 상품을 배송사에 전달해야 하는 기한
olist_order_items_dataset.csv,price,상품 가격
olist_order_items_dataset.csv,freight_value,배송비(운송 비용)
olist_order_payments_dataset.csv,order_id,주문을 식별하기 위한 고유 ID
olist_order_payments_dataset.csv,payment_sequential,결제 순서(결제와 관련된 순차적 번호)
olist_order_payments_dataset.csv,payment_type,결제 유형(예: credit_card, boleto 등)
olist_order_payments_dataset.csv,payment_installments,할부 횟수
olist_order_payments_dataset.csv,payment_value,결제 금액
olist_order_reviews_dataset.csv,review_id,리뷰를 식별하기 위한 고유 ID
olist_order_reviews_dataset.csv,order_id,주문을 식별하기 위한 고유 ID
olist_order_reviews_dataset.csv,review_score,리뷰 점수(예: 1~5점)
olist_order_reviews_dataset.csv,review_comment_title,리뷰 제목
olist_order_reviews_dataset.csv,review_comment_message,리뷰 코멘트 내용
olist_order_reviews_dataset.csv,review_creation_date,리뷰가 작성된 날짜
olist_order_reviews_dataset.csv,review_answer_timestamp,리뷰에 대한 답변이 작성된 날짜 및 시간
olist_orders_dataset.csv,order_id,주문을 식별하기 위한 고유 ID
olist_orders_dataset.csv,customer_id,고객을 식별하기 위한 고유 ID
olist_orders_dataset.csv,order_status,주문 상태(예: delivered, shipped, canceled 등)
olist_orders_dataset.csv,order_purchase_timestamp,주문이 생성된 날짜 및 시간
olist_orders_dataset.csv,order_approved_at,주문이 승인된 날짜 및 시간
olist_orders_dataset.csv,order_delivered_carrier_date,주문이 배송사에 전달된 날짜
olist_orders_dataset.csv,order_delivered_customer_date,주문이 고객에게 배송 완료된 날짜
olist_orders_dataset.csv,order_estimated_delivery_date,주문의 예상 배송 날짜
olist_products_dataset.csv,product_id,상품을 식별하기 위한 고유 ID
olist_products_dataset.csv,product_category_name,상품 카테고리 이름(원어)
olist_products_dataset.csv,product_name_lenght,상품 이름의 길이(문자 수)
olist_products_dataset.csv,product_description_lenght,상품 설명의 길이(문자 수)
olist_products_dataset.csv,product_photos_qty,상품 사진 개수
olist_products_dataset.csv,product_weight_g,상품 무게(g)
olist_products_dataset.csv,product_length_cm,상품 길이(cm)
olist_products_dataset.csv,product_height_cm,상품 높이(cm)
olist_products_dataset.csv,product_width_cm,상품 너비(cm)
olist_sellers_dataset.csv,seller_id,판매자를 식별하기 위한 고유 ID
olist_sellers_dataset.csv,seller_zip_code_prefix,판매자의 우편번호 접두사
olist_sellers_dataset.csv,seller_city,판매자가 위치한 도시
olist_sellers_dataset.csv,seller_state,판매자가 위치한 주(state)
product_category_name_translation.csv,product_category_name,상품 카테고리 이름(원어)
product_category_name_translation.csv,product_category_name_english,상품 카테고리 이름(영어)
"""

if __name__=="__main__":
    output_file = "./artifact/column_description.csv"
    generate_column_description(description_data, output_file)