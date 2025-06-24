from .base import *
from utils.config import *

from transformers.models.auto.tokenization_auto import AutoTokenizer
from transformers.pipelines import pipeline

import torch
import gc
import time
import pandas as pd


class Translator(BasePipeline):
    def __init__(self, config: TranslatePipelineConfig):        
        self.config = config
        self.batch_size = self.config.initial_batch_size

        # 모델에 기본 tokenizer가 지정되어 있지 않을수가 있으므로 명시적으로 지정
        tokenizer = AutoTokenizer.from_pretrained(self.config.checkpoint)
        tokenizer.padding_side = "left"

        self.pipeline = pipeline(
            "text-generation",
            model=self.config.checkpoint,
            tokenizer=tokenizer,
            torch_dtype=torch.bfloat16,
            device_map=self.config.device
        )

    def set_input(self, dataset: Iterable[Any]):
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

                print(f"[{self.config.device}] Processing batch: {end_index}/{total} - Time: {duration:.2f}s. Saved at {self.config.dst_path}")

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

def run_translator(config: TranslatePipelineConfig, dataset):
    translatore = Translator(config)
    translatore.set_input(dataset)
    translatore.run()