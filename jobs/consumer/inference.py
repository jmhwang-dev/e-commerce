import json
import time
from pprint import pformat
from confluent_kafka import Consumer
from transformers import pipeline
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

consumer_conf = {
    'bootstrap.servers': 'kafka1:9092,kafka2:9092,kafka3:9092',
    'group.id': 'translation-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['reviews.translation-prompts'])

try:
    for _ in range(10):  # 초기 할당 대기 루프
        assignments = consumer.assignment()
        if assignments:
            logging.info(f'할당된 파티션: {assignments}')
            break
        consumer.poll(0.1)
        time.sleep(5)
        logging.info("파티션 할당 대기 중...")

    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logging.info(f'컨슈머 오류: {msg.error()}')
            break

        value = json.loads(msg.value().decode('utf-8'))
        with open("/jobs/output.json", 'w') as f:
            json.dump(value, f)
        logging.info(f"\n{pformat(value, indent=2)}")
        
except KeyboardInterrupt:
    pass
finally:
    consumer.close()