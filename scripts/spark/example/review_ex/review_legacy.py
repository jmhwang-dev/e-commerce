import os, time
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import KafkaException
from confluent_kafka.error import SerializationError

import threading, logging

logger = logging.getLogger(__name__)
_init_lock = threading.Lock()

_sr, _avro_ser, _producer = None, None, None
SR_URL = "http://schema-registry:8081"
BOOTSTRAP = "kafka1:9092"
TOPIC = "review"

def _lazy_init(max_retry=10):
    global _sr, _avro_ser, _producer

    if _producer:
        return

    with _init_lock:
        if _producer:
            return

        # 1) 필수 env 검증
        if not SR_URL:
            raise ValueError("SCHEMA_REGISTRY_URL 미설정")
        if not BOOTSTRAP:
            raise ValueError("KAFKA_BOOTSTRAP 미설정")

        # 2) Schema Registry 클라이언트 생성
        sr_conf = {"url": SR_URL}
        _sr = SchemaRegistryClient(sr_conf)

        # 3) 스키마 조회 & 재시도
        for attempt in range(1, max_retry + 1):
            try:
                reg = _sr.get_latest_version(f"{TOPIC}-value")
                schema_str = reg.schema.schema_str
                logger.info(f"스키마 조회 성공: subject={TOPIC}-value v{reg.version} id={reg.schema_id}")
                break
            except Exception as e:
                logger.warning(f"스키마 조회 실패 (시도 {attempt}): {e}")
                time.sleep(min(2 ** attempt, 30))
        else:
            raise RuntimeError("스키마 조회 재시도 한계 도달")

        # 4) AvroSerializer 생성
        serializer_configs = {
            "auto.register.schemas": os.getenv("AUTO_REGISTER_SCHEMAS", "false") == "true",
            "use.latest.version":    os.getenv("USE_LATEST_VERSION", "true") == "true"
        }
        _avro_ser = AvroSerializer(
            _sr,
            schema_str,
            to_dict=lambda record, ctx: record,
            conf=serializer_configs
        )

        # 5) Kafka 프로듀서 생성
        p_conf = {
            "bootstrap.servers": BOOTSTRAP,
            "enable.idempotence": True,
            "acks": "all",
            "retries": 5,
            "compression.type": "lz4"
        }
        _producer = SerializingBronzeProducer(p_conf)

def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(f"Delivered to {msg.topic()}[{msg.partition()}]")

def start_polling(producer, interval=1.0):
    def poll_loop():
        producer.poll(0)
        # 다음 타이머 예약
        t = threading.Timer(interval, poll_loop)
        t.daemon = True           # 생성 후에 daemon 속성 설정
        t.start()

    # 첫 호출
    t0 = threading.Timer(interval, poll_loop)
    t0.daemon = True
    t0.start()

def publish_review(records: list[dict]):
    _lazy_init()
    start_polling(_producer, interval=0.5)

    for idx, record in enumerate(records, 1):
        context = SerializationContext(TOPIC, MessageField.VALUE)
        try:
            serialized = _avro_ser(record, context)
            _producer.produce(
                topic=TOPIC,
                key=record["review_id"],
                value=serialized,
                on_delivery=delivery_report
            )
        except (KafkaException, SerializationError) as e:
            logger.warning(f"Error preparing record {record['review_id']}: {e}")

        if idx % 100 == 0:
            try:
                _producer.flush(10)
            except KafkaException as e:
                logger.error(f"Flush error: {e}")

    # 마지막 flush
    try:
        _producer.flush(10)
    except KafkaException as e:
        logger.error(f"Final flush error: {e}")