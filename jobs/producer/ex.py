from producer.init import *
from producer.topic import *
from producer.publish import *


if __name__=="__main__":
    TOPIC = 'test-topic'
    delete_topics([TOPIC])
    create_topics([TOPIC])

    simulate_stream(
        PROCESS_TYPE.STREAM,
        TOPIC,
        'order_id',
        'delivery_estimated.tsv'
        )