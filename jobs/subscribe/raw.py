from service.init.spark import *
from service.init.kafka import *
from service.init.iceberg import *
from service.io.iceberg_spark import *

from service.consumer.raw import *
from service.init.iceberg import *

if __name__=="__main__":
    spark_session = get_spark_session("RawStream")

    for topic_name, table_identifier in BronzeLayer():
        kafka_stream_df = get_kafka_stream_df(spark_session, topic_name)
        decoded_stream_df = get_decoded_stream_df(kafka_stream_df, topic_name)
        load_stream(spark_session, decoded_stream_df, table_identifier)


    # qeury = start_console_stream(decoded_stream_df)
    # query = load_stream(spark_session, decoded_stream_df, BronzeLayer.REVIEW_TABLE_IDENTIFIER)
    # query.awaitTermination()

    try:
        spark_session.streams.awaitAnyTermination()  # 모든 쿼리 종료까지 대기
    except Exception as e:
        print(f"Streaming query failed: {e}")

    # preprocessed_review 토픽 만들고
    # 전처리 해서 inference_review로 발행

    # 리뷰 제외한 모든 토픽을 형변환해서 실버로
        # int여도 되는 float -> int

    # 번역 추론하고 실버로


    # 실버토픽에 토픽별로 집계해서 골드로 저장

    # 시각화(수퍼셋, 프로메테우스, 그라파나)

    # [옵션]
    # 원본 데이터로 브론즈 넣고
    # 실버에 redfined 형태로 넣기