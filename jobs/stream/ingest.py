
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from service.stream.topic import BronzeTopic
from service.utils.schema.reader import AvscReader
from service.utils.schema.registry_manager import SchemaRegistryManager
from service.utils.spark import *

SRC_TOPIC_NAMES = BronzeTopic.get_all_topics()

def load_medallion_layer(micro_batch_df:DataFrame, batch_id: int):
    topics_in_batch = [row.topic for row in micro_batch_df.select("topic").distinct().collect()]
    
    print(f"Processing Batch ID: {batch_id}")
    print(f"Topics in Batch: {topics_in_batch}")
    print()
    for topic_name in topics_in_batch:
        try:
            avsc_reader = AvscReader(topic_name)
            topic_df = micro_batch_df.filter(col("topic") == topic_name)
            deserialized_df = get_deserialized_stream_df(topic_df, avsc_reader.schema_str)

            record_count = deserialized_df.count()
            if record_count == 0:
                print(f"No records to write for topic {topic_name} in this batch.")
                continue
            
            print(f"Writing {record_count} rows to Iceberg table: {avsc_reader.dst_table_identifier}")
            deserialized_df.write \
                .format("iceberg") \
                .mode("append") \
                .saveAsTable(avsc_reader.dst_table_identifier)

        except Exception as e:
            print(f"Error processing topic {topic_name} in batch {batch_id}: {e}")

if __name__ == "__main__":
    spark_session = get_spark_session("Load CDC to bronze layer Job")
    client = SchemaRegistryManager._get_client(use_internal=True)
    src_stream_df = get_kafka_stream_df(spark_session, SRC_TOPIC_NAMES)

    query = src_stream_df.writeStream \
        .foreachBatch(load_medallion_layer) \
        .queryName("load_cdc") \
        .option("checkpointLocation", f"s3a://warehousedev/bronze/checkpoints") \
        .trigger(processingTime="10 seconds") \
        .start()
        
    query.awaitTermination()