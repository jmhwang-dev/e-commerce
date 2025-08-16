from pyspark.sql.dataframe import DataFrame

from service.common.topic import *
from service.producer.base import *
from service.utils.iceberg.spark import *


class GeolocationSilverProducer(BaseProducer):
    topic = BronzeToSilverTopic.GEOLOCATION
    pk_column = ['zip_code']

class CustomerSilverProducer(BaseProducer):
    topic = BronzeToSilverTopic.CUSTOMER
    pk_column = ['customer_id']

class SellerSilverProducer(BaseProducer):
    topic = BronzeToSilverTopic.SELLER
    pk_column = ['seller_id']

class ProductSilverProducer(BaseProducer):
    topic = BronzeToSilverTopic.PRODUCT
    pk_column = ['product_id']

class OrderStatusSilverProducer(BaseProducer):
    topic = BronzeToSilverTopic.ORDER_STATUS
    pk_column = ['order_id', 'status']

class PaymentSilverProducer(BaseProducer):
    topic = BronzeToSilverTopic.PAYMENT
    pk_column = ['order_id', 'payment_sequential']

class OrderItemSilverProducer(BaseProducer):
    topic = BronzeToSilverTopic.ORDER_ITEM
    pk_column = ['order_id', 'order_item_id']

class EstimatedDeliberyDateSilverProducer(BaseProducer):
    topic = BronzeToSilverTopic.ESTIMATED_DELIVERY_DATE
    pk_column = ['order_id']

class ReviewCleanCommentSilverProducer(BaseProducer):
    topic = BronzeToSilverTopic.REVIEW_CLEAN_COMMENT
    pk_column = ['review_id']
    use_internal = True

    @classmethod
    def publish(cls, spark_df: DataFrame):
        """PySpark DataFrame을 Kafka 토픽으로 스트리밍"""

        cls.producer = cls._get_producer(use_internal=True)
        
        def write_to_kafka(batch_df, batch_id):
            """각 배치를 Kafka로 전송"""
            if batch_df.isEmpty():
                print(f'\nEmpty batch {batch_id}: {cls.topic}')
                return
                            
            # DataFrame을 Row 단위로 처리
            for row in batch_df.collect():
                # Row를 dict로 변환
                event = row.asDict()
                
                # 디버깅: 실제 데이터 구조 확인
                print(f"Event data: {event}")
                print(f"Event keys: {list(event.keys())}")
                
                # 키 생성 (BaseProducer와 동일한 방식)
                key_str_list = []
                for pk_col in cls.pk_column:
                    if pk_col in event:
                        key_str_list.append(str(event[pk_col]))
                    else:
                        print(f"Warning: pk_column '{pk_col}' not found in event")
                        key_str_list.append("unknown")
                
                key = '|'.join(key_str_list)
                
                try:
                    cls.producer.produce(cls.topic, key=key, value=event)
                    cls.producer.flush()
                    print(f"✓ Successfully sent to {cls.topic}")
                except Exception as e:
                    print(f"✗ Failed to send to {cls.topic}: {e}")
                    print(f"Event that failed: {event}")
                    raise
                
                print(f'\nPublished message to {cls.topic} - key: {key}')
            
            print(f"Completed batch {batch_id} to {cls.topic}")
            print()
        
        # 스트림 쿼리 생성
        try:
            query = spark_df.writeStream \
                .foreachBatch(write_to_kafka) \
                .trigger(processingTime="10 seconds") \
                .start()
            
            return query
            
        except Exception as e:
            print(f"Failed to setup streaming to {cls.topic}: {e}")
            raise e