import torch
from transformers import pipeline
from util.loader import *
import time

if __name__ == "__main__":
    DST_PATH = "./translate/artifact/translated_cpu.txt"

    dataset = load_dataset("./translate/artifact/all_portuguess.txt")
    messages = load_messages(dataset[len(dataset)//2:])

    pipe = pipeline(
        "text-generation",
        model="Unbabel/TowerInstruct-7B-v0.2",
        torch_dtype=torch.bfloat16,
        device_map="cpu"
    )

    pipe.tokenizer.padding_side = 'left'
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
    