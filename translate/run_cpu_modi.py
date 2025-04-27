from loader import *
from pipeline import Translator
from config import Config

import time

if __name__ == "__main__":

    dataset = load_dataset("./translate/artifact/all_portuguess.txt")
    messages = load_messages(dataset[len(dataset)//2:])

    # 사용
    config_auto = Config(batch_size=8, device='auto')
    config_cpu = Config(batch_size=50, device='cpu')

    translator = Translator(config_auto)
    translator = Translator(config_cpu)
    print(config_auto)
    print(config_cpu)


    exit()
    prompts = pipe.tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)

    i = 0
    batch_size_ = 50
    start = time.time()
    while i < len(prompts):
        try:
            inference_start = time.time()
            outputs = pipe(
                prompts[i:i+batch_size_],
                max_new_tokens=512,
                do_sample=False,
                batch_size=batch_size_
            )
            inference_end = time.time()

            with open(DST_PATH, 'a') as f:
                for output in outputs:
                    kor = output[0]['generated_text'].split("<|im_start|>assistant\n")[-1].strip()
                    f.write(f"{kor}\n")
            
            print(f"{i+batch_size_} / {len(prompts)} ... complete: {inference_end - inference_start}")
            i += batch_size_
            batch_size_ = min(batch_size_ + 1, 2048)  # 상한선 

        except RuntimeError as e:  # CPU에서의 메모리 부족 처리
            print(f"[⚠️ RuntimeError] batch_size={batch_size_} ↓ 줄임")
            batch_size_ = max(1, batch_size_ - 1)
            import gc
            gc.collect()
    end = time.time()
    print(f"Done: {end-start}")
    