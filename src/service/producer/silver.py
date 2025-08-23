from service.producer.base import *
from service.common.topic import *
from service.utils.kafka import get_confluent_kafka_producer
from pyspark.sql.dataframe import DataFrame

class SilverProducer(BaseProducer):
    topic_dlq: Optional[str] = None  # DLQ topic (서브클래스에서 오버라이드 가능)
    dlq_producer: Optional[SerializingProducer] = None

    @classmethod
    def _get_or_create_producer(cls, topic: Optional[str], use_internal: bool = True) -> SerializingProducer:
        # Producer 캐싱 (클래스 변수 대신 lazy init)
        if topic == cls.topic_dlq:
            if cls.dlq_producer is None:
                cls.dlq_producer = get_confluent_kafka_producer(None, use_internal=use_internal)  # DLQ는 schema None (JSON)
            return cls.dlq_producer
        else:
            if cls.main_producer is None:
                cls.main_producer = get_confluent_kafka_producer(cls.topic, use_internal=use_internal)  # Avro
            return cls.main_producer

    @classmethod
    def publish(cls, spark_df: DataFrame, is_dlq: bool = False):
        """PySpark DataFrame을 Kafka 토픽으로 스트리밍 (DLQ fallback 포함)"""
        target_topic = cls.topic_dlq if is_dlq else cls.topic

        def write_to_kafka(batch_df: DataFrame, batch_id: int):
            if batch_df.rdd.isEmpty():
                print(f'Empty batch {batch_id}: {target_topic}')
                return

            def process_partition(rows):
                producer = cls._get_or_create_producer(target_topic)
                dlq_producer = cls._get_or_create_producer(cls.topic_dlq) if cls.topic_dlq else None

                for row in rows:
                    event = row.asDict()
                    key_values = [str(event.get(pk_col, 'unknown')) for pk_col in cls.pk_column]
                    key = '_'.join(key_values)  # str로 유지! encode 제거

                    try:
                        if is_dlq:
                            value = json.dumps(event)  # DLQ: str (JSON string), serializer가 encode
                        else:
                            value = event  # Clean: dict, AvroSerializer가 처리
                        producer.produce(target_topic, key=key, value=value)
                        print(f"✓ Sent to {target_topic}")
                    except Exception as e:
                        print(f"✗ Failed to send to {target_topic}: {e}")
                        if dlq_producer and not is_dlq:
                            try:
                                dlq_value = json.dumps(event)  # str로
                                dlq_producer.produce(cls.topic_dlq, key=key, value=dlq_value)
                                print(f"✓ Sent to DLQ: {cls.topic_dlq}")
                            except Exception as dlq_e:
                                print(f"✗ DLQ failed: {dlq_e}")
                                raise
                        else:
                            raise

                producer.flush()  # 파티션 단위 flush

            batch_df.foreachPartition(process_partition)
            print(f'Completed batch {batch_id+1}: {target_topic}')

        try:
            query = spark_df.writeStream \
                .foreachBatch(write_to_kafka) \
                .trigger(processingTime="15 seconds") \
                .start()
            return query
        except Exception as e:
            print(f"Failed to setup streaming to {target_topic}: {e}")
            raise e

class PaymentSilverProducer(SilverProducer):
    topic = SilverTopic.PAYMENT
    topic_dlq = SilverTopic.PAYMENT_DLQ
    pk_column = ['order_id', 'payment_sequential']

class ReviewCleanCommentSilverProducer(SilverProducer):
    topic = SilverTopic.REVIEW_CLEAN_COMMENT
    pk_column = ['review_id']

class ReviewMetadataSilverProducer(SilverProducer):
    topic = SilverTopic.REVIEW_METADATA
    pk_column = ['review_id']

class GeolocationSilverProducer(SilverProducer):
    topic = SilverTopic.GEOLOCATION
    pk_column = ['zip_code']

class CustomerSilverProducer(SilverProducer):
    topic = SilverTopic.CUSTOMER
    pk_column = ['customer_id']

class SellerSilverProducer(SilverProducer):
    topic = SilverTopic.SELLER
    pk_column = ['seller_id']

class ProductSilverProducer(SilverProducer):
    topic = SilverTopic.PRODUCT
    pk_column = ['product_id']

class OrderStatusSilverProducer(SilverProducer):
    topic = SilverTopic.ORDER_STATUS
    pk_column = ['order_id', 'status']

class OrderItemSilverProducer(SilverProducer):
    topic = SilverTopic.ORDER_ITEM
    pk_column = ['order_id', 'order_item_id']

class EstimatedDeliberyDateSilverProducer(SilverProducer):
    topic = SilverTopic.ESTIMATED_DELIVERY_DATE
    pk_column = ['order_id']