import socket
import time
import ray
import torch
from transformers import pipeline

# hostname = socket.gethostname()
# if hostname == 'desktop':
#     ray.init(address="auto", num_cpus=16, num_gpus=1,)
# elif hostname == "mini-pc":
#     ray.init(address="auto", num_cpus=4, num_gpus=1)
# else:
#     ray.init(address="auto", num_cpus=12, num_gpus=0)

ray.init(address='auto')

# Ray Actor로 파이프라인 생성 및 관리
@ray.remote(num_gpus=1)
class Translator:
    def __init__(self):
        self.pipe = pipeline(
            "text-generation",
            model="Unbabel/TowerInstruct-7B-v0.2",
            torch_dtype=torch.bfloat16,
            device_map="auto"
        )

    def translate_to_korean(self, messages):
        prompt = self.pipe.tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
        outputs = self.pipe(prompt, max_new_tokens=256, do_sample=False)
        return outputs[0]["generated_text"]

# Translator Actor 생성
translator = Translator.remote()

# 예시 문장
messages = [
    {"role": "user", "content": "Translate the following text from Portuguese into Korean.\nPortuguese: muito bom\nKorean:"},
]

start = time.time()
# Ray Actor 메서드 호출
results = ray.get([translator.translate_to_korean.remote(msg) for msg in messages])
end = time.time()

print(f"소요시간: {end - start} 초, 번역 결과: {results}")
