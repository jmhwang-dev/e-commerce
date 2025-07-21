from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from pathlib import Path
import json
import os

BOOTSTRAP = "kafka1:9092"
TOPIC     = "event-review-received"

def publish_review(record: dict):
    _sr = SchemaRegistryClient({"url": os.getenv("SCHEMA_REGISTRY_URL")})
    _schema_str = _sr.get_latest_version(f"{TOPIC}-value").schema.schema_str
    _avro_ser = AvroSerializer(_sr, _schema_str, to_dict=lambda o,_: o)

    _producer = SerializingProducer({
        "bootstrap.servers": BOOTSTRAP,
        "key.serializer": StringSerializer(),
        "value.serializer": _avro_ser
    })
    # ctx = SerializationContext(TOPIC, MessageField.VALUE)
    _producer.produce(
        topic=TOPIC,
        key=record["review_id"],
        value=record,
        on_delivery=lambda e, m: print("❌", e) if e else None
    )
    _producer.poll(0)
