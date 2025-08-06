from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from fastavro.schema import load_schema

import os

TOPIC = "review"
def publish_review(records: list[dict]):
    # 1) Schema Registry 클라이언트 설정
    sr_conf = {'url': 'http://schema-registry:8081'}
    schema_registry_client = SchemaRegistryClient(sr_conf)
    schema_avro = schema_registry_client.get_latest_version(f"{TOPIC}-value").schema.schema_str
    # schema_avro = load_schema("/mnt/shared/schemas/review.avsc")

    # 2) AvroSerializer 생성
    #    - 스키마는 subject 이름으로 레지스트리에서 자동 로드
    avro_serializer = AvroSerializer(
        schema_registry_client,
        schema_avro,  # None으로 두면 subject 기반으로 fetch
        to_dict=lambda obj, ctx: obj,
        conf={'auto.register.schemas': False}  # 필요시 True로 등록도 가능
    )

    # 3) SerializingProducer 설정
    producer_conf = {
        'bootstrap.servers': "kafka1:9092",
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer
    }
    producer = SerializingProducer(producer_conf)
    # context = SerializationContext(TOPIC, MessageField.VALUE)

    for record in records:
        producer.produce(
            topic="review",
            key=record["review_id"],
            value=record  # AvroSerializer가 dict → Avro bytes + header로 처리
        )
    producer.flush()
    print("Confluent Avro 메시지 전송 완료.")
