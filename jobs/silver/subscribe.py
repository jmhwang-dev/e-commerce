from service.utils.spark import get_spark_session, get_decoded_stream_df, get_kafka_stream_df, start_console_stream
from service.common.schema import SchemaRegistryManager
from service.producer.silver import *
from service.consumer.aggregate import *
from pyspark.sql.functions import col

if __name__ == "__main__":
    spark_session = get_spark_session("RawStream")
    client = SchemaRegistryManager._get_client(use_internal=True)
    # all_topic_names = SilverTopic.get_all_topics()
    all_topic_names = [SilverTopic.REVIEW_INFERED]
    kafka_stream_df = get_kafka_stream_df(spark_session, all_topic_names)

    queries = []
    for topic_name in all_topic_names:
        try:
            schema_str = client.get_latest_version(topic_name).schema.schema_str
            topic_filtered_df = kafka_stream_df.filter(col("topic") == topic_name)
            decoded_stream_df = get_decoded_stream_df(topic_filtered_df, schema_str)

            if topic_name == SilverTopic.REVIEW_INFERED:
                main_sentiment_df = get_main_setiment(decoded_stream_df)
                query = start_console_stream(main_sentiment_df)
                query.awaitTermination()
                

                # queries.append(query)

        except Exception as e:
            print(f"Failed to process topic {topic_name}: {e}")
    
    try:
        # TODO: query.status polling (실패 쿼리 미리 감지)
        # TODO: streaming metrics를 활용해 모니터링을 추가
        spark_session.streams.awaitAnyTermination()
    except Exception as e:
        print(f"Streaming failed: {e}")