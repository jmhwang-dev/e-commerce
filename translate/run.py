from pipeline import Translator
from config import *
from loader import *
import torch
import gc
import time
import os

if __name__ == "__main__":

    config = PipelineConfig(
        src_path=os.path.join(ARTIFACT_PATH, "all_portuguess.txt"),
        dst_path=os.path.join(ARTIFACT_PATH, "translated_auto.txt"),

        device='auto',
        batch_size=8,
        dynamic_batch_size_increment=1,
        dynamic_batch_size_decrement=1
    )
    config.save()
    exit()

    dataset = load_text(config.src_path)
    messages = text2message(dataset[len(dataset)//2:])

    translator = Translator(config)
    translator.set_prompts(messages)

    i = 0
    batch_size_ = config.batch_size
    start = time.time()
    while i < len(dataset):
        try:
            inference_start = time.time()
            translator.run(i, batch_size_)
            inference_end = time.time()
            print(f"{i+batch_size_} / {len(dataset)} ... complete: {inference_end - inference_start}")

            translator.save_outputs()
            i += batch_size_
            batch_size_ = min(batch_size_ + 1, 2048)  # 상한선 

        except RuntimeError as e:  # CPU에서의 메모리 부족 처리
            print(f"[⚠️ RuntimeError] batch_size={batch_size_} ↓ {config.dynamic_batch_size_decrement} 줄임")
            batch_size_ = max(1, batch_size_ - 1)
            gc.collect()

        except torch.cuda.OutOfMemoryError as e:
            print(f"[⚠️ OOM] batch_size={batch_size_} ↓ {config.dynamic_batch_size_decrement} 줄임")
            batch_size_ = max(1, batch_size_ - 1)
            torch.cuda.empty_cache()
            gc.collect()
            
    end = time.time()
    print(f"Done: {end-start}")