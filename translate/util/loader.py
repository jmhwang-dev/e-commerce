from transformers import pipeline
import torch
import os
from typing import List

def load_dataset(dataset_path) -> List:
    dataset = []
    with open(dataset_path, 'r') as f:
        for portuguese in f.readlines():
            dataset.append(portuguese.strip())
    return dataset

def load_messages(dataset) -> List:
    messages = []
    for text in dataset:
        msg = [{
            "role": "user",
            "content": f"Translate the following text from Portuguese into Korean.\nPortuguese: {text}\nKorean:"
        },
        ]

        messages.append(msg)
    return messages