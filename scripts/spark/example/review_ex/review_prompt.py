import json
from confluent_kafka import Producer

# 프로듀서 설정 (Kafka 3.9.1 호환)
producer_conf = {
    'bootstrap.servers': 'kafka1:9092,kafka2:9092,kafka3:9092'  # 포트포워딩에 맞게 수정 (예시)
}

# Producer 초기화
producer = BronzeProducer(producer_conf)

# JSON 데이터 예시 (prompt를 dict로 수정; 리스트 필요 시 되돌림)
message_value = {
    'review_id': 'f7c4243c7fe1938f181bec41a392bdeb',
    'comment_type': 'comment',
    'prompt': [{
        'role': 'user',
        'content': 'Translate the following text from Portuguese into English.\nPortuguese: parabéns lojas lannister adorei comprar pela internet seguro e prático parabéns a todos feliz páscoa\nEnglish:'
    }]
}

# 콜백 함수 (전송 결과 확인)
def delivery_report(err, msg):
    if err is not None:
        print(f'메시지 전송 실패: {err}')
    else:
        print(f'메시지 전송 성공: {msg.topic()} [{msg.partition()}]')

# 메시지 퍼블리싱
topic = 'reviews.translation-prompts'
try:
    producer.produce(
        topic=topic,
        key=message_value['review_id'].encode('utf-8'),  # 키를 bytes로 변환
        value=json.dumps(message_value).encode('utf-8'),  # value를 JSON 문자열로 직렬화 후 bytes 변환
        callback=delivery_report
    )
    producer.poll(10)  # 콜백 처리 대기 시간 증가
    producer.flush()  # 전송 완료 대기
except Exception as e:
    print(f'프로듀서 오류: {e}')