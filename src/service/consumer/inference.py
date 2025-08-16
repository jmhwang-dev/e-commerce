from confluent_kafka import Consumer
from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline, AutoModelForSequenceClassification
import torch
import time

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

def get_prompt(text: str) -> str:
    language_from = "Portuguese"
    language_into = "English"
    
    return [
        [{
            "role": "user",
            "content": f"Translate the following text from {language_from} into {language_into}.\n{language_from}: {text}\n{language_into}:"
        }]
    ]

def wait_for_partition_assignment(consumer):
    max_attempts = 10
    for _ in range(max_attempts):
        if consumer.assignment():
            print('Consumer partition assignment loaded!')
            return consumer
        consumer.poll(1)
        time.sleep(5)
    raise TimeoutError("Consumer 파티션 할당 실패")