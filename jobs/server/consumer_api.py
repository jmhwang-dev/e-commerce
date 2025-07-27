from fastapi import FastAPI, HTTPException, Response
import uuid, logging
import time
from services.consumer.translate import *

logger = logging.getLogger("consumer-on-demand")
logging.basicConfig(level=logging.INFO)

app = FastAPI()
group = f"on-demand-{uuid.uuid4()}"
consumer = create_consumer(group)
transltor = create_translator()

@app.get("/consume")
def consume_once():
    meta = consumer.list_topics(timeout=5.0)
    print("Available topics:", list(meta.topics.keys()))
    consumer.subscribe([TOPIC])
    
    # 1) 그룹 가입 및 파티션 할당 기다리기
    assignment = []
    for _ in range(5):           # 최대 5회 재시도
        consumer.poll(1.0)
        assignment = consumer.assignment()
        if assignment:
            break
        time.sleep(0.2)

    # logger.info(f"Assigned partitions: {assignment}")
    if not assignment:
        consumer.close()
        raise HTTPException(500, detail="No partitions assigned. Check connectivity or broker logs.")

    # 2) 원하는 오프셋(여기선 0)으로 이동
    tp = TopicPartition(assignment[0].topic, assignment[0].partition, 0)
    consumer.assign([tp])

    # 3) 실제 메시지 폴링
    msg = consumer.poll(timeout=2.0)
    consumer.close()

    if msg is None:
        return Response(status_code=204)
    if msg.error():
        raise HTTPException(502, detail=f"Kafka error: {msg.error()}")
    
    message = msg.value()
    chunk = get_prompt(message['review_comment_title'])
    outputs = transltor(
        chunk,
        max_new_tokens=512,
        do_sample=False,
        batch_size=len(chunk)
    )

    return {"record": outputs}