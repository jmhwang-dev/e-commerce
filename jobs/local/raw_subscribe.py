from service.init.kafka import *

if __name__=="__main__":
    consumer = get_consumer(BOOTSTRAP_SERVERS_EXTERNAL, ['review'])

    try:
        for message in consumer:
            print(message)
        
    except TimeoutError as e:
        print(str(e))
    except KeyboardInterrupt as e:
        print(str(e))
    finally:
        consumer.close()