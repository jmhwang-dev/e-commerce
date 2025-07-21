from fastapi import FastAPI, HTTPException
from domain.reviews import Review
from services.producer.review import publish_review
import json, pathlib

app = FastAPI(title="Review Producer API")
SAMPLE_PATH = pathlib.Path("/mnt/shared/sample_messages/review_raw.json")

@app.post("/produce")
def produce_review(review: Review):
    try:
        publish_review(review.model_dump())
        return {"status": "queued"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/produce/sample")
def produce_sample():
    record = json.loads(SAMPLE_PATH.read_text())
    print(record)
    publish_review(record)
    return {"status": "sample queued", "review_id": record["review_id"]}
