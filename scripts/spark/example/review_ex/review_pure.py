from kafka import KafkaProducer
from fastavro import schemaless_writer
from fastavro.schema import load_schema
import io

# Avro 스키마 로드
schema_avro = load_schema("/mnt/shared/schemas/review.avsc")
BOOTSTRAP = "kafka1:9092"
TOPIC = "review"

# Avro payload 직렬화 (헤더 없이 순수 payload만)
def to_avro_bytes(record):
    buf = io.BytesIO()
    schemaless_writer(buf, schema_avro, record)
    return buf.getvalue()

def publish_review(records: list[dict]):
    # KafkaProducer 설정

    print( schema_avro )
    exit()
    producer = KafkaBronzeProducer(
        bootstrap_servers=[BOOTSTRAP],
        key_serializer=lambda k: k.encode('utf-8'),
        value_serializer=lambda v: to_avro_bytes(v)
    )

    # 메시지 전송
    for record in records:
        producer.send(
            topic=TOPIC,
            key=record["review_id"],
            value=record
        )
    producer.flush()