import torch
from transformers import pipeline
from config import Config

class Translator():
    def __init__(self, config:Config):
        self.pipeline = pipeline(
            "text-generation",
            model=config.checkpoint,
            torch_dtype=torch.bfloat16,
            device_map=config.device
        )
        self.pipeline.tokenizer.padding_side = 'left'
        