import socket
import time
import ray
import torch
from transformers import pipeline

hostname = socket.gethostname()
if hostname == 'desktop':
    ray.init(address="auto", num_cpus=16, num_gpus=1)  # 1 GPU 사용
elif hostname == "mini-pc":
    ray.init(address="auto", num_cpus=4, num_gpus=1)  # 1 GPU 사용
else:
    ray.init(address="auto", num_cpus=12, num_gpus=0)  # 1 GPU 사용

pipe = pipeline("text-generation", model="Unbabel/TowerInstruct-7B-v0.2", torch_dtype=torch.bfloat16, device_map="auto")


# 번역 함수 (GPU 사용)
def translate_to_korean(messages):
    prompt = pipe.tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    outputs = pipe(prompt, max_new_tokens=256, do_sample=False)
    return outputs[0]["generated_text"]

# 5만 개 문장 예시
messages = [
    {"role": "user", "content": "Translate the following text from Portuguese into Korean.\nPortuguese: muito bom\nKorean:"},
]

# Ray를 사용하여 번역 작업 분배
start = time.time()
results = ray.get([ray.remote(translate_to_korean).remote(msg) for msg in messages])
end = time.time()

print("소요시간: {} 번역 결과: {}".format(end-start, results))  # 일부 결과만 출력