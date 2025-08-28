from service.common.topic import SilverTopic
from service.producer.base.pandas import PandasProducer

class ReviewConflictSentimentSilverProducer(PandasProducer):
    topic = SilverTopic.REVIEW_CONFLICT_SENTIMENT
    pk_column = ['review_id']

class ReviewConsistentSentimentSilverProducer(PandasProducer):
    topic = SilverTopic.REVIEW_CONSISTENT_SENTIMENT
    pk_column = ['review_id']