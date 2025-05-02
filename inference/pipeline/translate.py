from typing import List
from transformers import pipeline

from inference.pipeline.base import *
from config import *

class Translator(BasePipeline):
    def __init__(self, config:TranslatePipelineConfig):
        if os.path.exists(config.dst_path):
            raise FileExistsError
        
        self.config = config
        self.current_batch_size = self.config.batch_size

        self.pipeline = pipeline(
            "text-generation",
            model=self.config.checkpoint,
            torch_dtype=torch.bfloat16,
            device_map=self.config.device
        )
        self.pipeline.tokenizer.padding_side = 'left'

    def set_input(self, dataset:List[str]):
        messages = []
        for text in dataset:
            message = [{
                "role": "user",
                "content": f"Translate the following text from {self.config.language_from} into {self.config.language_into}.\n{self.config.language_from}: {text}\n{self.config.language_into}:"
            }]
            messages.append(message)
        self.prompts = self.pipeline.tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)

    def run(self,):
        try:
            inference_start = time.time()
            self.outputs = self.pipeline(
                    self.prompts,
                    max_new_tokens=512,
                    do_sample=False,
                    batch_size=self.current_batch_size
                )
            inference_end = time.time()
            print(f"batch size: {batch_size_} / {len(self.prompts)} ... complete: {inference_end - inference_start}")
            
        except RuntimeError as e:  # CPU에서의 메모리 부족 처리
            print(f"[⚠️ RuntimeError] batch_size={batch_size_} ↓ {self.config.dynamic_batch_size_decrement} 줄임")
            batch_size_ = max(1, batch_size_ - 1)
            gc.collect()

        except torch.cuda.OutOfMemoryError as e:
            print(f"[⚠️ OOM] batch_size={batch_size_} ↓ {self.config.dynamic_batch_size_decrement} 줄임")
            batch_size_ = max(1, batch_size_ - 1)
            torch.cuda.empty_cache()
            gc.collect()
        
    def get_results(self,):
        results = []
        for output in self.outputs:
            result = output[0]['generated_text'].split("<|im_start|>assistant\n")[-1].strip()
            results.append(result)