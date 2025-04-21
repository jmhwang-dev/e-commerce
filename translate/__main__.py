# Install transformers from source - only needed for versions <= v4.34
# pip install git+https://github.com/huggingface/transformers.git
# pip install accelerate

import torch
from transformers import pipeline
import pandas as pd
import socket
from tqdm import tqdm

class Translator():
    def __init__(self):
        self.pipe = pipeline("text-generation", model="Unbabel/TowerInstruct-7B-v0.2", torch_dtype=torch.bfloat16, device_map="auto")

    def set_messages(self, portuguess: str) -> None:
        self.messages = [
            {
                "role": "user",
                "content": rf"Translate the following text from Portuguese into Korean.\nPortuguese: {portuguess}\nKorean:"
            }
        ]
        
    def run(self,) -> None:
        prompt = self.pipe.tokenizer.apply_chat_template(self.messages, tokenize=False, add_generation_prompt=True)
        outputs = self.pipe(prompt, max_new_tokens=512, do_sample=False)
        self.output = outputs[0]['generated_text'].split("<|im_start|>assistant\n")[-1].strip()

    def get_output(self,) -> str:
        return self.output
    
def save(dst, origin, trans):
    with open(dst, 'a') as f:
        f.write(f"{origin} ===> {trans}\n")
    

if __name__=="__main__":
    df = pd.read_csv("./eda/review/artifact/reviews_preprocessed.csv", index_col=0)

    if socket.gethostname() == 'desktop':
        target_list = df['preprocessed_comment'].dropna().unique()
        dst_path = "./translate/translated_comment.txt"
    else:
        target_list = df['preprocessed_title'].dropna().unique()
        dst_path = "./translate/translated_title.txt"


    result = {}
    translator = Translator()

    for portuguess in tqdm(target_list, desc="Processing", ncols=70, ascii=True, leave=True):
        translator.set_messages(portuguess)
        translator.run()
        korean = translator.get_output()
        save(dst_path, portuguess, korean)