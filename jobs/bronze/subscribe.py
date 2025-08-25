from service.common.topic import *
from service.utils.spark import *
from service.consumer.review import *
from service.consumer.payment import *
from service.producer.silver import *

from pyspark.sql.functions import col
from functools import reduce

if __name__ == "__main__":
    spark_session = get_spark_session("RawStream")
    client = SchemaRegistryManager._get_client(use_internal=True)
    all_topic_names = BronzeTopic.get_all_topics()
    kafka_stream_df = get_kafka_stream_df(spark_session, all_topic_names)

    queries = []
    for topic_name in all_topic_names:
        try:
            schema_str = client.get_latest_version(topic_name).schema.schema_str
            topic_filtered_df = kafka_stream_df.filter(col("topic") == topic_name)
            decoded_stream_df = get_decoded_stream_df(topic_filtered_df, schema_str)

            if topic_name == BronzeTopic.PAYMENT:
                # DLQ: null 행
                null_condition = reduce(lambda x, y: x | y, [col(c).isNull() for c in decoded_stream_df.columns])
                dlq_df = decoded_stream_df.filter(null_condition)
                query_dlq = PaymentSilverProducer.publish(dlq_df, is_dlq=True)
                queries.append(query_dlq)

                # Clean: 모든 컬럼 not null
                clean_df = decoded_stream_df.dropna(how='any')
                transformed_clean_df = float2int(clean_df, ["payment_sequential", 'payment_value', 'payment_installments'])
                query_clean = PaymentSilverProducer.publish(transformed_clean_df, is_dlq=False)
                queries.append(query_clean)

            elif topic_name == BronzeTopic.REVIEW:
                review_metadata_df = get_review_metadata(decoded_stream_df)
                query_review_metadata = ReviewMetadataSilverProducer.publish(review_metadata_df, is_dlq=False)
                queries.append(query_review_metadata)

                melted_msg_df = PortuguessPreprocessor.melt_reviews(decoded_stream_df)
                clean_msg_df = PortuguessPreprocessor.clean_review_comment(melted_msg_df)
                query_clean_review = ReviewCleanCommentSilverProducer.publish(clean_msg_df, is_dlq=False)
                queries.append(query_clean_review)

            elif topic_name == BronzeTopic.ORDER_STATUS:
                query = OrderStatusSilverProducer.publish(decoded_stream_df, is_dlq=False)
            elif topic_name == BronzeTopic.GEOLOCATION:
                query = GeolocationSilverProducer.publish(decoded_stream_df, is_dlq=False)
            elif topic_name == BronzeTopic.CUSTOMER:            
                query = CustomerSilverProducer.publish(decoded_stream_df, is_dlq=False)
            elif topic_name == BronzeTopic.SELLER:            
                query = SellerSilverProducer.publish(decoded_stream_df, is_dlq=False)
            elif topic_name == BronzeTopic.PRODUCT:            
                query = ProductSilverProducer.publish(decoded_stream_df, is_dlq=False)
            elif topic_name == BronzeTopic.ORDER_STATUS:
                query = OrderStatusSilverProducer.publish(decoded_stream_df, is_dlq=False)
            elif topic_name == BronzeTopic.ORDER_ITEM:
                query = OrderItemSilverProducer.publish(decoded_stream_df, is_dlq=False)
            elif topic_name == BronzeTopic.ESTIMATED_DELIVERY_DATE:
                query = EstimatedDeliberyDateSilverProducer.publish(decoded_stream_df, is_dlq=False)

            queries.append(query)

        except Exception as e:
            print(f"Failed to process topic {topic_name}: {e}")
    
    try:
        spark_session.streams.awaitAnyTermination()
    except Exception as e:
        print(f"Streaming failed: {e}")