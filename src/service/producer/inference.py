from service.common.topic import SilverTopic
from service.producer.base import BaseProducer

class ReviewInferedSilverProducer(BaseProducer):
    topic = SilverTopic.REVIEW_INFERED
    pk_column = ['review_id']