from service.common.topic import BronzeTopic
from service.utils.spark import get_spark_session, get_decoded_stream_df, get_kafka_stream_df, start_console_stream
from service.pipeline.review import PortuguessPreprocessor, get_review_metadata
from service.consumer.payment import float2int
from service.utils.schema.registry_manager import SchemaRegistryManager
from service.producer.silver import *
from service.pipeline.silver import *

from pyspark.sql.functions import col
from functools import reduce

if __name__ == "__main__":
    spark_session = get_spark_session("RawStream")
    client = SchemaRegistryManager._get_client(use_internal=True)
    stream_topic_names = BronzeTopic.get_stream_topics()
    kafka_stream_df = get_kafka_stream_df(spark_session, stream_topic_names)

    queries = []
    for topic_name in stream_topic_names:
        try:
            schema_str = client.get_latest_version(topic_name).schema.schema_str
            topic_filtered_df = kafka_stream_df.filter(col("topic") == topic_name)
            decoded_stream_df = get_decoded_stream_df(topic_filtered_df, schema_str)

            # dev mode
            # if topic_name not in [BronzeTopic.REVIEW, ]:
            #     continue

            # TODO: 아래 클래스 정리
            # OrderStatusSilverJob,
            # ReviewSilverJob
            # PaymentSilverJob
            if topic_name == BronzeTopic.PAYMENT:
                # DLQ: null 행
                null_condition = reduce(lambda x, y: x | y, [col(c).isNull() for c in decoded_stream_df.columns])
                dlq_df = decoded_stream_df.filter(null_condition)
                query_dlq = PaymentSilverProducer.publish(dlq_df)
                queries.append(query_dlq)

                # Clean: 모든 컬럼 not null
                clean_df = decoded_stream_df.dropna(how='any')
                transformed_clean_df = float2int(clean_df, ["payment_sequential", 'payment_value', 'payment_installments'])

                query_clean = PaymentSilverProducer.publish(transformed_clean_df)
                queries.append(query_clean)



            elif topic_name == BronzeTopic.REVIEW:
                review_metadata_df = get_review_metadata(decoded_stream_df)
                query_review_metadata = ReviewMetadataSilverProducer.publish(review_metadata_df)
                queries.append(query_review_metadata)

                melted_msg_df = PortuguessPreprocessor.melt_reviews(decoded_stream_df)
                clean_msg_df = PortuguessPreprocessor.clean_review_comment(melted_msg_df)
                query_clean_review = ReviewCleanCommentSilverProducer.publish(clean_msg_df)
                queries.append(query_clean_review)

            elif topic_name == BronzeTopic.ORDER_STATUS:
                query = OrderStatusSilverProducer.publish(decoded_stream_df)

            queries.append(query)

        except Exception as e:
            print(f"Failed to process topic {topic_name}: {e}")
    
    try:
        # TODO: query.status polling (실패 쿼리 미리 감지)
        # TODO: streaming metrics를 활용해 모니터링을 추가
        spark_session.streams.awaitAnyTermination()
    except Exception as e:
        print(f"Streaming failed: {e}")