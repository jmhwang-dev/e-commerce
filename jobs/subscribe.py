from service.init import *
from service.consumer.subscribe import *

if __name__=="__main__":
    try:
        consumer = get_consumer(Topic.GEOLOCATION)
        consume_messages(consumer)
    except TimeoutError as e:
        print(str(e))
    except KeyboardInterrupt as e:
        print(str(e))