from pyspark.sql import DataFrame

from service.producer.bronze import *
from service.utils.spark import get_spark_session, get_kafka_stream_df, get_deserialized_stream_df, start_console_stream
from service.utils.schema.reader import AvscReader
from service.stream.helper import *

SPARK_SESSION = get_spark_session("Process stream")
BRONZE_NAMESPACE = 'bronze'
SILVER_NAMESPACE = 'silver'
GOLD_NAMESPACE = 'gold'

def load_iceberg(stream_df: DataFrame, namespace:str, table_name: str):
    table_identifier = f"{namespace}.{table_name}"
    checkpoint_path = f"checkpoint/{table_identifier.replace('.', '/')}"

    # # TODO: partition 유무 성능 확인
    # SPARK_SESSION.sql(f"""
    #     CREATE TABLE IF NOT EXISTS {table_identifier}
    #     USING iceberg
    # """)

    return stream_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .toTable(table_identifier)

def drop_all_tables(namespace: str):
    for table in [row.tableName for row in SPARK_SESSION.sql(f'show tables in {namespace}').collect()]:
        SPARK_SESSION.sql(f'drop table if exists {namespace}.{table} purge')
        print(f'drop done: {namespace}.{table}')
        # SPARK_SESSION.sql(f'DESCRIBE FORMATTED {namespace}.{table}').show()

def reset():
    for namespace in [BRONZE_NAMESPACE, SILVER_NAMESPACE, GOLD_NAMESPACE]:
        SPARK_SESSION.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
        drop_all_tables(namespace)

def get_deser_stream_df(producer_class: BronzeProducer) -> DataFrame:
    stream_df = get_kafka_stream_df(SPARK_SESSION, producer_class.dst_topic)
    avsc_reader = AvscReader(producer_class.dst_topic)
    return get_deserialized_stream_df(stream_df, avsc_reader.schema_str, producer_class.key_column)

if __name__ == "__main__":
    reset()
    
    order_status_stream = get_deser_stream_df(OrderStatusBronzeProducer)    
    order_item_stream = get_deser_stream_df(OrderItemBronzeProducer)
    product_stream = get_deser_stream_df(ProductBronzeProducer)

    stream_dict = {
        OrderStatusBronzeProducer.dst_topic: order_status_stream,
        OrderItemBronzeProducer.dst_topic: order_item_stream,
        ProductBronzeProducer.dst_topic: product_stream,
    }
        
    try:
        queries = [load_iceberg(stream_df, BRONZE_NAMESPACE, table_name) 
                   for table_name, stream_df in stream_dict.items()]
        for query in queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping all streaming queries...")
        for query in SPARK_SESSION.streams.active:
            query.stop()