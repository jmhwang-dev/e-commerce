from typing import List

from pyspark.sql.streaming.query import StreamingQuery
from service.utils.spark import get_spark_session, run_stream_queries
from service.utils.iceberg import initialize_namespace
from service.utils.logger import *
from service.pipeline.stream.silver import *
from service.pipeline.stream.gold import *


LOGGER = get_logger(__name__, '/opt/spark/logs/stream.log')

QUERY_LIST: List[StreamingQuery] = []

if __name__ == "__main__":
    spark_session = get_spark_session("Stream Silver Job")
    # spark_session.sparkContext.setLogLevel("DEBUG")

    initialize_namespace(spark_session, 'silver', is_drop=True)

    stream_job_list:List[BaseJob] = []

    for job_class in [DimUserLocation, DimProduct, FactOrderReview, FactOrderItem, FactOrderTimeline]:
        stream_job_list += [job_class(spark_session)]
    
    for job_instance in stream_job_list:
        QUERY_LIST += [job_instance.get_query(process_time='10 seconds')]

    # TODO: inference
    # review_comment = review_stream_df.select('review_id', 'review_comment_title', 'review_comment_message').dropna()

    run_stream_queries(spark_session, QUERY_LIST, LOGGER)