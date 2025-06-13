from typing import List
from abc import ABC, abstractmethod

from transformers import pipeline

import torch
import gc
import time
import pandas as pd


class BasePipeline():
    @abstractmethod
    def __init__(self):
        pass
    
    @abstractmethod
    def set_input(dataset:List[str]):
        pass

    @abstractmethod
    def run():
        pass

    @abstractmethod
    def get_results() -> List:
        pass