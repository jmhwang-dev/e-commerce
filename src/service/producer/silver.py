from service.producer.base.spark import SparkProducer
from service.stream.topic import SilverTopic

class ReviewCleanCommentSilverProducer(SparkProducer):
    dst_topic = SilverTopic.REVIEW_CLEAN_COMMENT
    key_column = ['review_id']

class ReviewMetadataSilverProducer(SparkProducer):
    dst_topic = SilverTopic.REVIEW_METADATA
    key_column = ['review_id']