from service.init.spark import *
from service.init.kafka import *
from service.init.iceberg import *
from service.io.iceberg import *

from service.consumer.stream import *
from service.io.iceberg import *

if __name__=="__main__":
    spark_session = get_spark_session("RawStream")
    topic_name = 'review'
    kafka_stream_df = get_kafka_stream_df(spark_session, topic_name)
    decoded_stream_df = get_decoded_stream_df(kafka_stream_df, topic_name)

    # qeury = start_console_stream(decoded_stream_df)
    qeury = load_stream(decoded_stream_df)
    qeury.awaitTermination()