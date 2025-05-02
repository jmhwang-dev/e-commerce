from typing import List
from transformers import pipeline

from inference.pipeline.base import *
from config import *

class Translator(BasePipeline):
    def __init__(self, config:TranslatePipelineConfig):        
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
            print(f"Processing batch: {self.current_batch_size}/{len(self.prompts)} - Time: {inference_end - inference_start:.2f}s")


        except RuntimeError as e:
            print(f"{str(e)}", end=': ')
            self.decrease_batch_size()
            self.current_batch_size = max(1, self.current_batch_size)
            gc.collect()

        except torch.cuda.OutOfMemoryError as e:
            print(f"{str(e)}", end=': ')
            self.decrease_batch_size()
            self.current_batch_size = max(1, self.current_batch_size)
            torch.cuda.empty_cache()
            gc.collect()
        
    def get_results(self,) -> List[str]:
        results = []
        for output in self.outputs:
            result = output[0]['generated_text'].split("<|im_start|>assistant\n")[-1].strip()            
            results.append(result)
        return results

    def increase_batch_size(self, ):
        print(f"Batch size has increased from {self.current_batch_size} to {self.current_batch_size + self.config.dynamic_batch_size_increment}")
        self.current_batch_size += self.config.dynamic_batch_size_increment

    def decrease_batch_size(self, ):
        print(f"Batch size has decreased from {self.current_batch_size} to {self.current_batch_size - self.config.dynamic_batch_size_decrement}")
        self.current_batch_size -= self.config.dynamic_batch_size_decrement