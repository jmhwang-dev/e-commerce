from service.init.spark import *
from service.init.kafka import *
from service.init.iceberg import *
from service.io.iceberg_spark import *

from service.consumer.stream import *

if __name__=="__main__":
    spark_session = get_spark_session("RawStream")
    topic_name = 'review'
    kafka_stream_df = get_kafka_stream_df(spark_session, topic_name)
    decoded_stream_df = get_decoded_stream_df(kafka_stream_df, topic_name)

    # 스파크로 아이스버그 table 만들고


    # 모든 토픽 넣기: 브론즈
    # review_inference 토픽 만들고
    # 전처리 해서 review_inference로 발행

    # 리뷰 제외한 모든 토픽을 형변환해서 실버로
        # int여도 되는 float -> int

    # 번역 추론하고 실버로


    # 실버토픽에 토픽별로 집계해서 골드로 저장

    # 시각화(수퍼셋, 프로메테우스, 그라파나)

    # [옵션]
    # 원본 데이터로 브론즈 넣고
    # 실버에 redfined 형태로 넣기

    qeury = start_console_stream(decoded_stream_df)
    # qeury = load_stream(decoded_stream_df)
    qeury.awaitTermination()