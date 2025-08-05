from transformers import pipeline
import torch

# from transformers.models.auto.tokenization_auto import AutoTokenizer, AutoModelForCausalLM
from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline, AutoModelForSequenceClassification

def get_sentiment_analyzer():
    model_path = "/models/sentiment"

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
        tokenizer=tokenizer,  # 여기서 tokenizer 명시
        top_k=3,
        max_length=512,
        truncation=True,  # 입력 초과 시 자르기 (권장)
        return_all_scores=True  # 모든 클래스 확률 반환 (권장)
    )
    # analyzer = pipeline(
    #     "text-classification",
    #     model=model,
    #     device='cpu',
    #     top_k=3,
    #     max_length=512,
    #     truncation=False,
    #     )
    return analyzer

def get_translator():
    model_path = "/models/tower"  # 샤딩된 체크포인트 디렉토리

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

if __name__=="__main__":
    get_sentiment_analyzer()