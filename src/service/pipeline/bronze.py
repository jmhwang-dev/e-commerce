from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from service.utils.schema.reader import AvscReader
from service.common.topic import BronzeTopic
from service.utils.spark import get_decoded_stream_df

def process_micro_batch(micro_batch_df: DataFrame, batch_id: int):
    """
    이 함수는 스트리밍 쿼리의 각 마이크로배치에 대해 실행됩니다.
    1. 배치 내 고유한 토픽 목록을 식별합니다.
    2. 각 토픽별로 데이터를 필터링합니다.
    3. 해당 토픽의 스키마를 사용하여 Avro 데이터를 역직렬화합니다.
    4. 최종 데이터를 올바른 Iceberg 테이블에 저장합니다.
    """
    
    # 현재 마이크로배치에 포함된 고유한 토픽 목록을 가져옵니다.
    topics_in_batch = [row.topic for row in micro_batch_df.select("topic").distinct().collect()]
    
    print(f"Processing Batch ID: {batch_id}")
    print(f"Topics in Batch: {topics_in_batch}")
    print()
    
    # Schema Registry 클라이언트는 한 번만 가져옵니다.
    bronze_topics = BronzeTopic.get_all_topics()

    for topic_name in bronze_topics:
        try:
            avsc_reader = AvscReader(topic_name)
            topic_df = micro_batch_df.filter(col("topic") == topic_name)
            deserialized_df = get_decoded_stream_df(topic_df, avsc_reader.schema_str)        
            record_count = deserialized_df.count()
            if record_count > 0:
                print(f"Writing {record_count} rows to Iceberg table: {avsc_reader.dst_table_identifier}")
                deserialized_df.write \
                    .format("iceberg") \
                    .mode("append") \
                    .saveAsTable(avsc_reader.dst_table_identifier)
            else:
                print(f"No records to write for topic {topic_name} in this batch.")

        except Exception as e:
            print(f"Error processing topic {topic_name} in batch {batch_id}: {e}")