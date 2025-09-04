from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from service.common.topic import BronzeTopic, SilverTopic
from service.producer.bronze import *
from service.producer.silver import *

from service.utils.schema.reader import AvscReader
from service.pipeline.review import PortuguessPreprocessor, get_review_metadata
from service.consumer.payment import float2int


from functools import reduce


def transform_topic_stream(target_topic_names: List[str], micro_batch_df:DataFrame, batch_id: int):
    topics_in_batch = [row.topic for row in micro_batch_df.select("topic").distinct().collect()]
    
    print(f"Processing Batch ID: {batch_id}")
    print(f"Topics in Batch: {topics_in_batch}")
    print()

    queries = []
    for topic_name in target_topic_names:
        try:
            if topic_name == BronzeTopic.PAYMENT:
                # DLQ: null 행
                null_condition = reduce(lambda x, y: x | y, [col(c).isNull() for c in micro_batch_df.columns])
                dlq_df = micro_batch_df.filter(null_condition)
                query_dlq = PaymentSilverProducer.publish(dlq_df)
                queries.append(query_dlq)

                # Clean: 모든 컬럼 not null
                clean_df = micro_batch_df.dropna(how='any')
                transformed_clean_df = float2int(clean_df, ["payment_sequential", 'payment_value', 'payment_installments'])

                query_clean = PaymentSilverProducer.publish(transformed_clean_df)
                queries.append(query_clean)


            elif topic_name == BronzeTopic.REVIEW:
                review_metadata_df = get_review_metadata(micro_batch_df)
                query_review_metadata = ReviewMetadataSilverProducer.publish(review_metadata_df)
                queries.append(query_review_metadata)

                melted_msg_df = PortuguessPreprocessor.melt_reviews(micro_batch_df)
                clean_msg_df = PortuguessPreprocessor.clean_review_comment(melted_msg_df)
                query_clean_review = ReviewCleanCommentSilverProducer.publish(clean_msg_df)
                queries.append(query_clean_review)

            elif topic_name == BronzeTopic.ORDER_STATUS:
                query = OrderStatusSilverProducer.publish(micro_batch_df)

        except Exception as e:
            print(f"Error processing topic {topic_name} in batch {batch_id}: {e}")
