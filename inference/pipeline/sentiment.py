from typing import List
from transformers import pipeline, AutoModelForSequenceClassification, AutoTokenizer
import torch

from inference.pipeline.base import *
from config import *
from pprint import pprint

class SentimentAnalyzer(BasePipeline):
    def __init__(self, config:TranslatePipelineConfig):
        self.config = config
        self.current_batch_size = self.config.batch_size
        self.pipeline = pipeline("text-classification", model=self.config.checkpoint, top_k=3)

    def set_input(self, dataset:List[str]):
        self.texts = dataset

    def run(self,):
        try:
            inference_start = time.time()
            self.outputs = self.pipeline(self.texts)
            inference_end = time.time()
            print(f"dataset size: {len(self.texts)} ... complete: {inference_end - inference_start}")
            
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
        self.results = []
        for text, outputs in zip(self.texts, self.outputs):

            redefine_score = {'text': text.strip()}
            for output in outputs:
                redefine_score[output['label']] = output['score']
            self.results.append(redefine_score)
            
        return self.results