# producer/app/main.py

from fastapi import FastAPI, Request
from pydantic import BaseModel
from app.avro_encoder import encode_avro
from app.kafka_producer import send_message
import os

app = FastAPI()
TOPIC = os.getenv("KAFKA_TOPIC", "review_raw")

class Review(BaseModel):
    review_id: str
    order_id: str
    review_score: int
    review_comment_title: str | None = None
    review_comment_message: str | None = None
    review_creation_date: str
    review_answer_timestamp: str | None = None

@app.post("/send_review")
async def send_review(review: Review):
    avro_bytes = encode_avro(review.dict())
    send_message(TOPIC, avro_bytes)
    return {"status": "ok"}
