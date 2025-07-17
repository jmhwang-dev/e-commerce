# producer/app/kafka_producer.py

from confluent_kafka import Producer
import os
 
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092")

producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
})

def send_message(topic: str, value: bytes):
    producer.produce(topic, value=value)
    producer.flush()
