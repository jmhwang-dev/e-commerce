from typing import List, Iterable, Any
from abc import ABC, abstractmethod

from transformers.pipelines import pipeline

import torch
import gc
import time
import pandas as pd


class BasePipeline(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def set_input(self, dataset: Iterable[Any]):
        pass

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def get_results(self) -> List:
        pass