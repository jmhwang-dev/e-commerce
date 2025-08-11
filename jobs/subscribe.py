from service.init import *
from service.consumer.subscribe import *

if __name__=="__main__":
    try:
        topic_list = list(Topic().__iter__())
        consumer = get_consumer(topic_list)
        consume_messages(consumer)
    except TimeoutError as e:
        print(str(e))
    except KeyboardInterrupt as e:
        print(str(e))