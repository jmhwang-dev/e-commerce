import os, time
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP", "kafka1:9092")
TOPIC      = os.getenv("REVIEW_TOPIC",  "review")
SR_URL     = os.getenv("SCHEMA_REGISTRY_URL")

_sr          = None
_avro_ser    = None
_producer    = None

def _lazy_init(max_retry=10):
    """Schema Registry·Producer 지연 초기화 & 재시도"""
    global _sr, _avro_ser, _producer
    if _producer:
        return

    # Schema Registry 클라이언트
    _sr = SchemaRegistryClient({"url": SR_URL})
    for attempt in range(1, max_retry + 1):
        try:
            schema_str = _sr.get_latest_version(f"{TOPIC}-value").schema.schema_str
            break
        except Exception as e:
            if attempt == max_retry:
                raise RuntimeError(f"schema fetch failed after {max_retry} tries") from e
            time.sleep(2 ** attempt / 10)

    # Avro Serializer
    _avro_ser = AvroSerializer(_sr, schema_str, to_dict=lambda o, _: o)

    # Producer
    _producer = SerializingProducer({
        "bootstrap.servers": BOOTSTRAP,
        "key.serializer": StringSerializer(),
        "value.serializer": _avro_ser,
        "enable.idempotence": True,      # 중복 방지 옵션
    })

def publish_review(record: dict):
    _lazy_init()

    _producer.produce(
        topic=TOPIC,
        key=record["review_id"],
        value=record,
        on_delivery=lambda err, _msg: print("❌", err) if err else None
    )
    _producer.poll(0)    # 콜백 처리
    _producer.flush(0)   # 버퍼 비우기 (0 = non-blocking)
