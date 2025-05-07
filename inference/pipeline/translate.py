from inference.pipeline.base import *
from config import *
import gc
import time
from typing import List

class Translator(BasePipeline):
    def __init__(self, config: TranslatePipelineConfig):        
        self.config = config
        self.batch_size = self.config.initial_batch_size

        self.pipeline = pipeline(
            "text-generation",
            model=self.config.checkpoint,
            torch_dtype=torch.bfloat16,
            device_map=self.config.device
        )
        self.pipeline.tokenizer.padding_side = 'left'

    def set_input(self, dataset: List[str]):
        self.prompts = [
            [{
                "role": "user",
                "content": f"Translate the following text from {self.config.language_from} into {self.config.language_into}.\n{self.config.language_from}: {text}\n{self.config.language_into}:"
            }]
            for text in dataset
        ]

    def run(self):
        start_index = 0
        total = len(self.prompts)

        while start_index < total:
            end_index = min(start_index + self.batch_size, total)
            chunk = self.prompts[start_index:end_index]

            try:
                start_time = time.time()
                outputs = self.pipeline(
                    chunk,
                    max_new_tokens=512,
                    do_sample=False,
                    batch_size=len(chunk)
                )
                duration = time.time() - start_time

                print(f"[{self.config.device}] Processing batch: {end_index}/{total} - Time: {duration:.2f}s")

                self.save_results(outputs)
                start_index = end_index
                self.adjust_batch_size(+1)

            except torch.cuda.OutOfMemoryError:
                print(f"[{self.config.device}] ⚠️ Out of Memory. Reducing batch size.")
                self.adjust_batch_size(-1)
                torch.cuda.empty_cache()
                gc.collect()

    def adjust_batch_size(self, delta: int):
        if self.config.device == 'auto':
            new_size = max(1, self.batch_size + delta)
        else:
            # Limit maximum batch size for CPU inference
            new_size = min(80, self.batch_size + delta)
        print(f"[{self.config.device}] Batch size {'increased' if delta > 0 else 'decreased'}: {self.batch_size} → {new_size}")
        self.batch_size = new_size

    def save_results(self, outputs):
        results = [
            out[0]['generated_text'][-1]['content'] for out in outputs
        ]
        with open(self.config.dst_path, 'a', encoding='utf-8') as f:
            for result in results:
                f.write(result.strip() + "\n")
