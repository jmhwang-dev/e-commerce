from typing import List
from confluent_kafka import Consumer
from confluent_kafka import KafkaError

from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline, AutoModelForSequenceClassification
import torch
import time
import pandas as pd

TOPIC = 'reviews.translation-prompts'
CONSUMER_CONFIG = {
    'bootstrap.servers': 'kafka1:9092,kafka2:9092,kafka3:9092',
    'group.id': 'translation-consumer-group',
    'auto.offset.reset': 'earliest'
}

def get_sentiment_analyzer():
    model_path = "/mnt/models/sentiment"

    model = AutoModelForSequenceClassification.from_pretrained(
        model_path,
        device_map="cpu",
        attn_implementation="eager"
    )
    # 토크나이저 로드 (필수!)
    tokenizer = AutoTokenizer.from_pretrained(model_path)
    tokenizer.padding_side = "left"  # RoBERTa에 적합

    model.eval()

    analyzer = pipeline(
        "text-classification",
        model=model,
        tokenizer=tokenizer,
        top_k=3,
        max_length=512,
        truncation=False,  # 입력 초과 시 자르기 (권장)
    )
    return analyzer

def get_translator():
    model_path = "/mnt/models/translate"  # 샤딩된 체크포인트 디렉토리

    # 샤딩된 모델 로드
    model = AutoModelForCausalLM.from_pretrained(
        model_path,
        torch_dtype=torch.bfloat16,
        device_map="auto",
        trust_remote_code=False
    )
    model.eval()
    tokenizer = AutoTokenizer.from_pretrained(model_path)
    tokenizer.padding_side = "left"

    trasnlator = pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer,
        torch_dtype=torch.bfloat16,
        device_map='auto'
        )
    return trasnlator

def fetch_batch(consumer, num_message=10, timeout=1.0, max_wait_seconds=30):
    messages = []
    start_time = time.time()
    while len(messages) < num_message:
        if time.time() - start_time > max_wait_seconds and len(messages) >= 1:
            return messages  # 최소 크기 도달 시 반환
        
        msg = consumer.poll(timeout)
        if msg is None:
            time.sleep(0.1)  # 짧은 대기 후 재시도 (CPU 과부하 방지)
            continue
        
        messages.append(msg)  # Add to batch
    
    return messages

def message2dataframe(messages: List[dict]) -> pd.DataFrame:
    value_list = []
    for message in messages:
        # key = message.key()       # str
        value = message.value()     # dict
        value_list.append(value)
    
    return pd.DataFrame(value_list)

def get_prompts(text: str) -> List[dict]:
    language_from = "Portuguese"
    language_into = "English"
    return [{
        "role": "user",
        "content": f"Translate the following text from {language_from} into {language_into}.\n{language_from}: {text}\n{language_into}:"
    }]

def translate(translator, dataset: List[str]) -> pd.DataFrame:
    start = time.time()
    por2eng_outputs = translator(
        dataset,
        max_new_tokens=512,
        do_sample=False,
        batch_size=len(dataset)
    )
    end = time.time()
    print(f"Time: Translation for {len(dataset)} dataset: {end-start}")

    por2eng_results = [
        out[0]['generated_text'][-1]['content'] for out in por2eng_outputs
    ]
    por2eng_results = list(map(lambda x: x.strip(), por2eng_results))
    return pd.DataFrame(data=por2eng_results, columns=['eng'])

def analyze(analyzer, eng_text_list: List[str]) -> pd.DataFrame:
    start = time.time()
    senti_outputs = analyzer(
        eng_text_list,
        batch_size=len(eng_text_list))
    end = time.time()
    print(f"Time: Analyzation for {len(eng_text_list)} dataset: {end-start}")
    result = [{item['label']: item['score'] for item in row} for row in senti_outputs]
    return pd.DataFrame(result)

def get_sample_df():
    data = {
        'review_id': [
            '0b637962a16eb130be75783b53f7fa6d',
            '57998f6fb9bff624291f11c44033bf57',
            'b212b7df25b4bd6fdc43df8e43c6ed3c',
            '13eff5faa3978a8ce971ae19a9747862',
            'efef5a00aac989951350619e06681dcb'
        ],
        'message_type': ['review_comment_message'] * 5,
        'eng': [
            'product did not arrive I have not had any resp...',
            'for the shopping experience in the free market...',
            'I liked it arrived on time and the service was...',
            'very good price, but the delivery time is very...',
            'store garbage, delivered wrong product, sent e...'
        ],
        'negative': [0.998981, 0.001457, 0.000174, 0.989022, 0.998509],
        'neutral': [0.000806, 0.997438, 0.000185, 0.004993, 0.000806],
        'positive': [0.000213, 0.001106, 0.999640, 0.005985, 0.000685]
    }

    return pd.DataFrame(data)