from service.common.topic import *
from service.utils.iceberg.spark import *
from service.utils.spark import *
from service.consumer.review import *

from service.producer.silver import *
from pyspark.sql.functions import col
from service.consumer.payment import *

if __name__ == "__main__":
    spark_session = get_spark_session("LoadStream")
    client = SchemaRegistryManager._get_client(use_internal=True)
    all_topic_names = BronzeTopic.get_all_topics() + SilverTopic.get_all_topics()
    kafka_stream_df = get_kafka_stream_df(spark_session, all_topic_names)

    queries = []
    for topic_name in all_topic_names:
        try:
            schema_str = client.get_latest_version(topic_name).schema.schema_str
            topic_filtered_df = kafka_stream_df.filter(col("topic") == topic_name)
            decoded_stream_df = get_decoded_stream_df(topic_filtered_df, schema_str)
            query = write_stream_iceberg(spark_session, decoded_stream_df, schema_str)  # StreamingQuery 반환
            queries.append(query)

        except Exception as e:
            print(f"Failed to process topic {topic_name}: {e}")
    
    try:
        spark_session.streams.awaitAnyTermination()  # 루프 후 대기
    except Exception as e:
        print(f"Streaming {topic_name} failed: {e}")