from service.common.topic import *
from service.utils.iceberg.spark import *
from service.utils.spark import *
from service.consumer.review import *

from service.producer.silver import *
from pyspark.sql.functions import col, cast
from pyspark.sql.types import IntegerType, FloatType

from service.consumer.silver import float2int

if __name__ == "__main__":
    spark_session = get_spark_session("RawStream")
    client = SchemaRegistryManager._get_client(use_internal=True)
    all_topic_names = RawToBronzeTopic.get_all_topics()
    kafka_stream_df = get_kafka_stream_df(spark_session, all_topic_names)

    queries = []
    for raw2bronze_topic_name in all_topic_names:
        try:
            # 모든 토픽은 bronze에 전부 저장
            schema_str = client.get_latest_version(raw2bronze_topic_name).schema.schema_str
            topic_filtered_df = kafka_stream_df.filter(col("topic") == raw2bronze_topic_name)
            decoded_stream_df = get_decoded_stream_df(topic_filtered_df, schema_str)
            query = load_stream(spark_session, decoded_stream_df, schema_str)  # StreamingQuery 반환
            queries.append(query)

            if raw2bronze_topic_name  == RawToBronzeTopic.PAYMENT:
                # TODO: null 값 dlq로, 실버 스키마 변경 (payment_sequential, payment_value, payment_installments)
                transformed_df = float2int(decoded_stream_df, ["payment_sequential", 'payment_value', 'payment_installments'])
                payment_schema_str = client.get_latest_version(BronzeToSilverTopic.PAYMENT).schema.schema_str
                query_payment = load_stream(spark_session, transformed_df, payment_schema_str)
                queries.append(query_payment)

            elif raw2bronze_topic_name == RawToBronzeTopic.REVIEW:
                # review_metadata_df는 아이스버그에 저장한다. 스트림 X
                review_metatdata_schema_str = client.get_latest_version(BronzeToSilverTopic.REVIEW_METADATA).schema.schema_str
                review_metadata_df = review_metadata_bronze2silver(decoded_stream_df)
                query_review_metadata = load_stream(spark_session, review_metadata_df, review_metatdata_schema_str)
                queries.append(query_review_metadata)

                # clean_msg_df는 아이스버그, 스트림 모두 저장 -> 여기까지 성공한거였음
                review_clean_comment_schema_str = client.get_latest_version(BronzeToSilverTopic.REVIEW_CLEAN_COMMENT).schema.schema_str
                melted_msg_df = PortuguessPreprocessor.melt_reviews(decoded_stream_df)
                clean_msg_df = PortuguessPreprocessor.clean_review_comment(melted_msg_df)
                query_review_clean = load_stream(spark_session, clean_msg_df, review_clean_comment_schema_str)
                queries.append(query_review_clean)

                # clean_msg_df는 토픽 발행 부분
                query_review_clean_topic = ReviewCleanCommentSilverProducer.publish(clean_msg_df)
                queries.append(query_review_clean_topic)

        except Exception as e:
            print(f"Failed to process topic {raw2bronze_topic_name}: {e}")
    
    try:
        spark_session.streams.awaitAnyTermination()  # 루프 후 대기
    except Exception as e:
        print(f"Streaming {raw2bronze_topic_name} failed: {e}")