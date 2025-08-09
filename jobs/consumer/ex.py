from consumer.subscribe import *

if __name__=="__main__":
    try:
        consumer = get_consumer('test-topic')
        consume_messages(consumer)
    except TimeoutError as e:
        print(str(e))
    except KeyboardInterrupt as e:
        print(str(e))