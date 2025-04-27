from dataclasses import dataclass
import os

@dataclass(kw_only=True)
class BaseConfig:
    src_path: str
    dst_path: str

@dataclass(kw_only=True)
class PipelineConfig(BaseConfig):
    device: str
    batch_size: int
    dynamic_batch_size_increment: int
    dynamic_batch_size_decrement: int
    checkpoint: str = "Unbabel/TowerInstruct-7B-v0.2"

    def __post_init__(self):
        if self.dynamic_batch_size_decrement < 0:
            raise ValueError("dynamic_batch_size_decrement should be positive number.")
        
        if self.device not in ('auto', 'cpu'):
            raise ValueError("device should be one of 'auto' or 'cpu'.")