from service.init.spark import *
from service.init.kafka import *
from service.init.iceberg import *
from service.io.iceberg_spark import *

from service.consumer.utils import *
from service.consumer.review import *
from service.init.iceberg import *

from pyspark.sql.functions import col

if __name__ == "__main__":
    spark_session = get_spark_session("RawStream")
    client = SchemaRegistryManager._get_client(use_internal=True)
    all_topic_names = RawToBronzeTopic.get_all_topics()
    kafka_stream_df = get_kafka_stream_df(spark_session, all_topic_names, 'raw2bronze')

    queries = []
    for topic_name in all_topic_names:
        try:
            schema_str = client.get_latest_version(topic_name).schema.schema_str
            create_namespace(spark_session, schema_str)
            topic_filtered_df = kafka_stream_df.filter(col("topic") == topic_name)
            decoded_stream_df = get_decoded_stream_df(topic_filtered_df, schema_str)
            query = load_stream(decoded_stream_df, schema_str)  # StreamingQuery 반환
            queries.append(query)

        except Exception as e:
            print(f"Failed to process topic {topic_name}: {e}")
    
    try:
        spark_session.streams.awaitAnyTermination()  # 루프 후 대기
    except Exception as e:
        print(f"Streaming query failed: {e}")