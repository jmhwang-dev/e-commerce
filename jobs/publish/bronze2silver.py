from service.init.spark import *
from service.init.kafka import *
from service.init.iceberg import *
from service.io.iceberg_spark import *

from service.consumer.utils import *
from service.consumer.review import *

from service.init.iceberg import *
from service.producer.silver import *

if __name__ == "__main__":
    spark_session = get_spark_session("RawStream")
    client = SchemaRegistryManager._get_client(use_internal=True)

    all_topic_names = RawToBronzeTopic.get_all_topics()
    kafka_stream_df = get_kafka_stream_df(spark_session, all_topic_names, 'bronze2silver')

    queries = []
    for raw2bronze_topic_name in all_topic_names:
        try:
            schema_str = client.get_latest_version(raw2bronze_topic_name).schema.schema_str
            create_namespace(spark_session, schema_str)
            topic_filtered_df = kafka_stream_df.filter(col("topic") == raw2bronze_topic_name)
            decoded_stream_df = get_decoded_stream_df(topic_filtered_df, schema_str)

            if raw2bronze_topic_name == RawToBronzeTopic.REVIEW:
                review_metatdata_schema_str = client.get_latest_version(BronzeToSilverTopic.REVIEW_METADATA).schema.schema_str
                review_info = review_info_bronze2silver(decoded_stream_df)
                query_review_info = load_stream(review_info, review_metatdata_schema_str)
                queries.append(query_review_info)

                review_clean_comment_schema_str = client.get_latest_version(BronzeToSilverTopic.REVIEW_CLEAN_COMMENT).schema.schema_str
                melted_msg_df = PortuguessPreprocessor.melt_reviews(decoded_stream_df)
                clean_msg_df = PortuguessPreprocessor.clean_review_comment(melted_msg_df)                
                query_review_clean = load_stream(clean_msg_df, review_clean_comment_schema_str)
                ReviewCleanCommentSilverProducer.publish()
                queries.append(query_review_clean)

            elif raw2bronze_topic_name in []:
                # TODO: 현변환 필요한 데이터 처리
                # - 리뷰 제외한 모든 토픽을 형변환해서 실버로
                # - int여도 되는 float -> int
                pass

        except Exception as e:
            print(f"Failed to process topic {raw2bronze_topic_name}: {e}")
    
    try:
        spark_session.streams.awaitAnyTermination()  # 루프 후 대기
    except Exception as e:
        print(raw2bronze_topic_name, '6666666666666666')
        print(f"Streaming query failed: {e}")

    # 번역 추론하고 실버로

    # 실버토픽에 토픽별로 집계해서 골드로 저장

    # 시각화(수퍼셋, 프로메테우스, 그라파나)

    # [옵션]
    # 원본 데이터로 브론즈 넣고
    # 실버에 redfined 형태로 넣기