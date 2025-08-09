from consumer.init import *

def consume_messages(consumer):
    try:
        for message in consumer:
            print(f"Received - Key: {message.key}, Value: {message.value}")
            
    except KeyboardInterrupt:
        print("Consumer stopped")
    finally:
        consumer.close()