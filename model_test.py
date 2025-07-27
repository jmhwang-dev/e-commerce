from transformers import AutoModelForCausalLM, AutoTokenizer
from transformers.pipelines import pipeline

import torch

local_dir = "/Users/jmhwang/.cache/huggingface/hub/models--Unbabel--TowerInstruct-7B-v0.2/snapshots/a240dfd3a0990c859ecfd4671e0ba9b923c04a43"

tokenizer = AutoTokenizer.from_pretrained(local_dir, local_files_only=True)
tokenizer.padding_side = "left"

# model = AutoModelForCausalLM.from_pretrained(
#     local_dir,
#     torch_dtype=torch.bfloat16,     # 7B 모델용 메모리 최적화 예시
#     device_map="auto",              # GPU 자동 배치
#     local_files_only=True
# )

model = AutoModelForCausalLM.from_pretrained(
    local_dir,
    torch_dtype=torch.bfloat16,     # 7B 모델용 메모리 최적화 예시
    device_map="cpu",              # GPU 자동 배치
    local_files_only=True
)

p2e_pipe = pipeline(
    "text-generation",
    model=model,
    tokenizer=tokenizer,
    torch_dtype=torch.bfloat16,
    device_map='cpu'
)

language_from = "Portuguese"
language_into = 'English'

text = "Olá, mundo!"
chunk = [
    [{
        "role": "user",
        "content": f"Translate the following text from {language_from} into {language_into}.\n{language_from}: {text}\n{language_into}:"
    }]
]
outputs = p2e_pipe(chunk,
         max_new_tokens=512,
         do_sample=False,
         batch_size=len(chunk))

print(outputs[0]['generated_text']['content'])

# text = tokenizer("안녕, 세상!", return_tensors="pt").to(model.device)
# out  = model.generate(**text, max_new_tokens=50)
# print(tokenizer.decode(out[0], skip_special_tokens=True))
