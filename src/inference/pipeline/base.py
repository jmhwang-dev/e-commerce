from typing import Iterable, Any
from abc import ABC, abstractmethod

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

    # @abstractmethod
    # def get_results(self) -> List:
    #     pass