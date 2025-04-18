# Install transformers from source - only needed for versions <= v4.34
# pip install torch
# pip install transformers
# pip install accelerate

import torch
from transformers import pipeline

pipe = pipeline("text-generation", model="Unbabel/TowerInstruct-7B-v0.2", torch_dtype=torch.bfloat16, device_map="auto")
# We use the tokenizer’s chat template to format each message - see https://huggingface.co/docs/transformers/main/en/chat_templating
messages = [
    {"role": "user", "content": "Translate the following text from Portuguese into Korean.\nPortuguese: Um grupo de investigadores lançou um novo modelo para tarefas relacionadas com tradução.\nKorean:"},
]
prompt = pipe.tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
outputs = pipe(prompt, max_new_tokens=256, do_sample=False)
print(outputs[0]["generated_text"])