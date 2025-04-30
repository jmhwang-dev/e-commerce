import torch
from transformers import pipeline
from loader import *
from config import *

class Translator():
    def __init__(self, config:PipelineConfig):
        if os.path.exists(config.dst_path):
            raise FileExistsError
        
        self.config = config
        self.pipeline = pipeline(
            "text-generation",
            model=self.config.checkpoint,
            torch_dtype=torch.bfloat16,
            device_map=self.config.device
        )
        self.pipeline.tokenizer.padding_side = 'left'

    def set_prompts(self, messages:List):
        self.prompts = self.pipeline.tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)

    def run(self, start_index, batch_size):
        self.outputs = \
            self.pipeline(
                self.prompts[start_index:start_index+batch_size],
                max_new_tokens=512,
                do_sample=False,
                batch_size=batch_size
                )

    def save_outputs(self,):
        with open(self.config.dst_path, 'a') as f:
            for output in self.outputs:
                kor = output[0]['generated_text'].split("<|im_start|>assistant\n")[-1].strip()
                f.write(f"{kor}\n")