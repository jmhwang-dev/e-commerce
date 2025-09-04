# foreachBatch에 사용하는 함수 (transform.py)

from typing import List 
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, struct, concat_ws

from service.common.topic import BronzeTopic
from service.producer.silver import *
from service.utils.schema.reader import AvscReader
from service.utils.spark import get_decoded_stream_df
from service.pipeline.batch import *

# ⭐️ Spark 세션을 인자로 받도록 수정
def transform_topic_stream(micro_batch_df:DataFrame, batch_id: int, serializer_udfs: dict):
    topics_in_batch = [row.topic for row in micro_batch_df.select("topic").distinct().collect()]
    
    print(f"Processing Batch ID: {batch_id}, Topics: {topics_in_batch}")

    # ⭐️ 토픽 이름과 처리 로직을 매핑하는 딕셔너리를 사용하여 if/elif 체인을 제거
    JOB_MAP = {
        BronzeTopic.CUSTOMER: (CustomerSilverJob, CustomerSilverProducer),
        BronzeTopic.PAYMENT: (PaymentSilverJob, PaymentSilverProducer),
        BronzeTopic.ORDER_STATUS: (OrderStatusSilverJob, OrderStatusSilverProducer),
        # ... 다른 모든 단일 출력 토픽들을 여기에 추가 ...
    }

    for topic_name in topics_in_batch:
        try:
            avsc_reader = AvscReader(topic_name)
            topic_df = micro_batch_df.filter(col("topic") == topic_name)
            
            deserialized_df = get_decoded_stream_df(topic_df, avsc_reader.schema_str)        

            publish_map = {}

            # ⭐️ Review 토픽처럼 출력이 여러 개인 경우는 특별 처리
            if topic_name == BronzeTopic.REVIEW:
                destination_dfs = ReviewSilverJob().transform(deserialized_df)
                publish_map[ReviewCleanCommentSilverProducer] = destination_dfs.get(ReviewCleanCommentSilverProducer.topic)
                publish_map[ReviewMetadataSilverProducer] = destination_dfs.get(ReviewMetadataSilverProducer.topic)
            
            # ⭐️ 나머지 모든 토픽은 맵을 사용하여 일반적인 방식으로 처리
            elif topic_name in JOB_MAP:
                job_class, producer_class = JOB_MAP[topic_name]
                destination_dfs = job_class().transform(deserialized_df)
                publish_map[producer_class] = destination_dfs.get(producer_class.topic)

            if not publish_map or len(destination_dfs) == 0:
                continue

            for producer_class, transformed_df in publish_map.items():
                if transformed_df is None or transformed_df.isEmpty():
                    continue
                key_columns = producer_class.pk_column
                if isinstance(key_columns, str):
                    key_columns = [key_columns]
                
                # value_columns = [c for c in transformed_df.columns if c not in key_columns]
                
                # ⭐️ (수정) UDF를 목적지 Silver 토픽 이름으로 조회
                dest_topic_name = producer_class.topic
                serializer_udf = serializer_udfs.get(dest_topic_name)
                if not serializer_udf:
                    print(f"Warning: Serializer UDF for destination topic '{dest_topic_name}' not found. Skipping.")
                    continue

                df_to_serialize = transformed_df
                
                # 1. DataFrame의 스키마를 순회하며 TimestampType인 컬럼을 모두 찾습니다.
                for field in df_to_serialize.schema:
                    if isinstance(field.dataType, TimestampType):
                        print(f"Casting timestamp column '{field.name}' to string.")
                        df_to_serialize = df_to_serialize.withColumn(field.name, col(field.name).cast("string"))
                
                df_to_publish = df_to_serialize.select(
                    concat_ws("-", *[col(c).cast("string") for c in key_columns]).alias("key"),
                    # ⭐️ struct('*')를 사용하여 데이터프레임의 모든 컬럼을 value_struct로 만듭니다.
                    struct(*df_to_serialize.columns).alias("value_struct")
                )

                serialized_df = df_to_publish.withColumn(
                    "value", serializer_udf(col("value_struct"))
                )

                producer_class.publish(serialized_df.select("key", "value"))
                print(f"Published {transformed_df.count()} records for destination topic {dest_topic_name}")

        except Exception as e:
            print(f"Error processing source topic {topic_name} in batch {batch_id}: {e}")