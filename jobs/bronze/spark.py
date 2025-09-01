from service.common.topic import BronzeTopic
from service.common.schema import SchemaRegistryManager

from service.utils.iceberg.spark import *
from service.utils.spark import *

from pyspark.sql.functions import col

def process_micro_batch(micro_batch_df: DataFrame, batch_id: int):
    """
    이 함수는 스트리밍 쿼리의 각 마이크로배치에 대해 실행됩니다.
    1. 배치 내 고유한 토픽 목록을 식별합니다.
    2. 각 토픽별로 데이터를 필터링합니다.
    3. 해당 토픽의 스키마를 사용하여 Avro 데이터를 역직렬화합니다.
    4. 최종 데이터를 올바른 Iceberg 테이블에 저장합니다.
    """
    # # 성능 최적화를 위해 들어온 마이크로배치를 캐싱합니다.
    # micro_batch_df.persist()
    # micro_batch_spark_session = micro_batch_df.sparkSession
    
    # 현재 마이크로배치에 포함된 고유한 토픽 목록을 가져옵니다.
    topics_in_batch = [row.topic for row in micro_batch_df.select("topic").distinct().collect()]
    
    print(f"--- Processing Batch ID: {batch_id}, Topics in Batch: {topics_in_batch} ---")
    
    # Schema Registry 클라이언트는 한 번만 가져옵니다.
    client = SchemaRegistryManager._get_client(use_internal=True)
    bronze_topics = BronzeTopic.get_all_topics()

    for topic_name in bronze_topics:
        try:
            topic_df = micro_batch_df.filter(col("topic") == topic_name)
            schema_str = client.get_latest_version(topic_name).schema.schema_str
            if not schema_str:
                print(f"Skipping topic {topic_name} due to schema fetch failure.")
                continue

            deserialized_df = get_decoded_stream_df(topic_df, schema_str)
            s3_uri, table_identifier, table_name = get_iceberg_destination(schema_str)
            
            record_count = deserialized_df.count()
            if record_count > 0:
                print(f"Writing {record_count} rows to Iceberg table: {table_identifier}")
                # load_batch()
                deserialized_df.write \
                    .format("iceberg") \
                    .mode("append") \
                    .saveAsTable(table_identifier)
            else:
                print(f"No records to write for topic {topic_name} in this batch.")

        except Exception as e:
            print(f"Error processing topic {topic_name} in batch {batch_id}: {e}")

    # # 처리가 끝난 마이크로배치의 캐시를 해제합니다.
    # micro_batch_df.unpersist()

if __name__ == "__main__":
    spark_session = get_spark_session("BronzeJob")
    client = SchemaRegistryManager._get_client(use_internal=True)
    all_topic_names = BronzeTopic.get_all_topics()

    tmp_topic = all_topic_names[0]
    schema_str = client.get_latest_version(tmp_topic).schema.schema_str
    # # TODO: 여기서는 `s3_uri` 만 필요
    s3_uri, table_identifier, table_name = get_iceberg_destination(schema_str)
    # create_namespace(spark_session, schema_str)

    all_topic_stream_df = get_kafka_stream_df(spark_session, all_topic_names)
    base_stream_df = all_topic_stream_df.select(col("key").cast("string"), col("value"), col("topic"))

    query = base_stream_df.writeStream \
        .foreachBatch(process_micro_batch) \
        .queryName("Bronze_Loader") \
        .option("checkpointLocation", f"s3a://{s3_uri}/checkpoints") \
        .trigger(processingTime="10 seconds") \
        .start()
        
    query.awaitTermination()