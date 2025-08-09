from producer.init import *
from producer.topic import *
from producer.publish import *


if __name__=="__main__":
    delete_topics(['test', 'test-topic'])
    create_topics(['test'])

    # 예: delivery_status.tsv 스트림
    # simulate_stream('delivery_status.tsv', 'delivery_status')