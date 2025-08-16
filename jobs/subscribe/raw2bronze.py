# raw2bronze.py (modified)

from service.init.spark import *
from service.init.kafka import *
from service.init.iceberg import *
from service.io.iceberg_spark import *

from service.consumer.utils import *
from service.consumer.review import *

from service.init.iceberg import *

if __name__ == "__main__":
    spark_session = get_spark_session("RawStream")
    client = SchemaRegistryManager._get_client(use_internal=True)

    all_topic_names = RawToBronzeTopic.get_all_topics()
    kafka_stream_df = get_kafka_stream_df(spark_session, all_topic_names, 'raw2bronze')

    for topic_name in all_topic_names:
        try:
            if topic_name in ['cdc_customer', 'stream_estimated_delivery_date', 'cdc_geolocation', 'stream_review']:
                continue
            schema_str = client.get_latest_version(topic_name).schema.schema_str
            create_namespace(spark_session, schema_str)
            topic_filtered_df = kafka_stream_df.filter(col("topic") == topic_name)
            
            # Decode the filtered stream with the topic-specific schema
            decoded_stream_df = get_decoded_stream_df(topic_filtered_df, schema_str)
            load_stream(decoded_stream_df, schema_str)

            spark_session.streams.awaitAnyTermination()  # 모든 쿼리 종료까지 대기
        except Exception as e:
            print(topic_name, '6666666666666666')
            print(f"Streaming query failed: {e}")

    # v review_processed ,review_metadata 토픽 만들기
    # 전처리 해서 review_processed 발행

    # 리뷰 제외한 모든 토픽을 형변환해서 실버로
        # int여도 되는 float -> int

    # 번역 추론하고 실버로

    # 실버토픽에 토픽별로 집계해서 골드로 저장

    # 시각화(수퍼셋, 프로메테우스, 그라파나)

    # [옵션]
    # 원본 데이터로 브론즈 넣고
    # 실버에 redfined 형태로 넣기