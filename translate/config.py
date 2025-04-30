from typing import Union
from dataclasses import dataclass
from dataclasses import asdict

import os
import yaml

ARTIFACT_PATH = "./translate/artifact"

@dataclass(kw_only=True)
class BaseConfig:
    src_path: str
    dst_path: str = ""

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
        
        self.dst_path = os.path.join(ARTIFACT_PATH, f"translated_{self.device}.txt")

        
    def save(self,):
        save_path = os.path.join(ARTIFACT_PATH, f'config_{self.device}.yml')
        if os.path.exists(save_path):
            raise FileExistsError(f"{save_path} already exists.")
        
        config_dict = asdict(self)
        with open(save_path, 'w') as f:
            yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
            
        print(f"Configuration saved at {save_path}.")