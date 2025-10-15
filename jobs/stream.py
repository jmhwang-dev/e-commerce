from typing import List

from pyspark.sql.streaming.query import StreamingQuery
from service.producer.bronze import BronzeTopic
from service.utils.spark import get_spark_session, run_stream_queries
from service.utils.iceberg import load_stream_to_iceberg, initialize_namespace
from service.utils.logger import *

LOGGER = get_logger(__name__, '/opt/spark/logs/stream.log')

SRC_NAMESPACE = 'bronze'
DST_NAMESPACE = 'silver'

SPARK_SESSION = get_spark_session("ReviewMetadata")
QUERY_LIST: List[StreamingQuery] = []

if __name__ == "__main__":
    initialize_namespace(SPARK_SESSION, DST_NAMESPACE, is_drop=True)

    review_stream_df = SPARK_SESSION.readStream.format('iceberg').load(f'{SRC_NAMESPACE}.{BronzeTopic.REVIEW}')
    review_metadata = review_stream_df.select('review_id', 'order_id', 'review_score', 'review_creation_date', 'review_answer_timestamp')

    query = load_stream_to_iceberg(review_metadata, f"{DST_NAMESPACE}.review_metadata" )
    QUERY_LIST.append(query)
    
    # TODO: inference
    # review_comment = review_stream_df.select('review_id', 'review_comment_title', 'review_comment_message').dropna()
    
    run_stream_queries(SPARK_SESSION, QUERY_LIST, LOGGER)