from typing import List

from pyspark.sql.streaming.query import StreamingQuery
from service.producer.bronze import BronzeTopic
from service.utils.spark import get_spark_session, run_stream_queries
from service.utils.iceberg import initialize_namespace
from service.utils.logger import *
from service.pipeline.stream.silver import *

LOGGER = get_logger(__name__, '/opt/spark/logs/stream.log')

QUERY_LIST: List[StreamingQuery] = []

if __name__ == "__main__":
    spark_session = get_spark_session("Stream Silver Job")

    initialize_namespace(spark_session, 'silver', is_drop=True)
    initialize_namespace(spark_session, 'gold', is_drop=True)

    job_list:List[StreamSilverJob] = []

    for job_class in [Account]:
        job_list += [job_class(spark_session)]
    
    for job_instance in job_list:
        QUERY_LIST += [job_instance.get_query()]


    # review
    # review_stream_df = SPARK_SESSION.readStream.format('iceberg').load(f'{SRC_NAMESPACE}.{BronzeTopic.REVIEW}')
    # review_metadata = review_stream_df.select('review_id', 'order_id', 'review_score', 'review_creation_date', 'review_answer_timestamp')

    # query = load_stream_to_iceberg(review_metadata, f"{DST_NAMESPACE}.review_metadata" )
    
    # TODO: inference
    # review_comment = review_stream_df.select('review_id', 'review_comment_title', 'review_comment_message').dropna()

    run_stream_queries(spark_session, QUERY_LIST, LOGGER)