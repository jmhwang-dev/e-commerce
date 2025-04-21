# Install transformers from source - only needed for versions <= v4.34
# pip install git+https://github.com/huggingface/transformers.git
# pip install accelerate

import torch
from transformers import pipeline
import pandas as pd
import socket
from tqdm import tqdm
from multiprocessing import Process

class Translator():
    def __init__(self, device_:str='auto'):
        self.pipe = pipeline("text-generation", model="Unbabel/TowerInstruct-7B-v0.2", torch_dtype=torch.bfloat16, device_map=device_)

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
    
def run_with_multiprocessing(target_list, device_='auto'):
    translator = Translator(device_)

    for portuguess in tqdm(target_list, desc="Processing", ncols=70, ascii=True, leave=True):
        translator.set_messages(portuguess)
        translator.run()
        korean = translator.get_output()
        save(dst_path, portuguess, korean)

if __name__=="__main__":
    df = pd.read_csv("./eda/review/artifact/reviews_preprocessed.csv", index_col=0)

    if socket.gethostname() == 'desktop':
        target_list = df['preprocessed_comment'].dropna().unique()[:2]
        dst_path = "./translate/translated_comment.txt"

        cpu_cnt = 2
        chunk_size = len(target_list) // cpu_cnt

        process_list = []

        for i in range(cpu_cnt):
            if i < cpu_cnt - 1:
                data_range = target_list[i*chunk_size:i*chunk_size+chunk_size]
            else:
                data_range = target_list[i*chunk_size:]
            process_list.append(Process(target=run_with_multiprocessing, args=(data_range, 'cpu')))

    else:
        target_list = df['preprocessed_title'].dropna().unique()
        dst_path = "./translate/translated_title.txt"

        process1 = Process(target=run_with_multiprocessing, args=(target_list, 'cpu'))
        process_list = [process1]

    import time
    start = time.time()
    for process in process_list:
        process.start()

    for process in process_list:
        process.join()
    end = time.time()
    print("multiprocessing: ", end-start)

    start = time.time()
    run_with_multiprocessing(target_list[:10], 'cpu')
    end = time.time()
    print("1 cpu model: ", end-start)


    start = time.time()
    run_with_multiprocessing(target_list[:10], 'auto')
    end = time.time()
    print("1 gpu model: ", end-start)
