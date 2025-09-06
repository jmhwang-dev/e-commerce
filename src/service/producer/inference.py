from service.common.topic import InferenceTopic
from service.producer.base.pandas import PandasProducer

class ReviewConflictSentimentSilverProducer(PandasProducer):
    topic = InferenceTopic.REVIEW_CONFLICT_SENTIMENT
    pk_column = ['review_id']

class ReviewConsistentSentimentSilverProducer(PandasProducer):
    topic = InferenceTopic.REVIEW_CONSISTENT_SENTIMENT
    pk_column = ['review_id']