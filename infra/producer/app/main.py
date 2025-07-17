# app/main.py

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from kafka import KafkaProducer
import json
import os

app = FastAPI()

# 환경변수로 bootstrap 서버를 받아온다
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# Kafka 프로듀서 설정
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

class Message(BaseModel):
    key: str
    value: dict

@app.post("/publish")
async def publish_message(msg: Message):
    try:
        producer.send(
            topic=KAFKA_TOPIC,
            key=msg.key.encode("utf-8"),
            value=msg.value
        )
        return {"status": "sent", "topic": KAFKA_TOPIC}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# @app.post("/send_review")
# async def send_review(data: ReviewModel):  # Pydantic 기반
#     # 1. Avro 스키마 로드
#     # 2. data.dict() → Avro 직렬화
#     # 3. KafkaProducer.send('review_raw', value=encoded_avro)