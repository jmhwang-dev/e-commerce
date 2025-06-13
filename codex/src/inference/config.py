from typing import Union
from dataclasses import dataclass
from dataclasses import asdict

import os
import yaml

ARTIFACT_DIR = "./artifact"

@dataclass(kw_only=True)
class BaseConfig:
    src_path: str
    dst_dir_name: str
    dst_file_name: str
    inplace: bool = False

    def set_save_config_params(self,):
        self.config_dict = {
            "src_path": self.src_path,
            "dst_path": self.dst_path
        }
        
    def save(self,):
        dst_file_name_ = os.path.splitext(self.dst_file_name)[0]
        config_file_name = f"config_{dst_file_name_}.yml"
        config_path = os.path.join(self.current_artifact_dir, config_file_name)
        
        if not self.inplace and os.path.exists(config_path):
            raise FileExistsError(f"{config_path} already exists.")
        
        self.set_save_config_params()
        with open(config_path, 'w') as f:
            yaml.dump(self.config_dict, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
            
        print(f"Configuration saved at {config_path}.")

    def __post_init__(self):
        self.current_artifact_dir = os.path.join(ARTIFACT_DIR, self.dst_dir_name)
        self.dst_path = os.path.join(self.current_artifact_dir, self.dst_file_name)
        
        if not self.inplace and os.path.exists(self.dst_path):
            raise FileExistsError(f"{self.dst_path} already exists.")
        
        os.makedirs(self.current_artifact_dir, exist_ok=True)


@dataclass(kw_only=True)
class PipelineConfig(BaseConfig):
    checkpoint: str
    device: str = 'cpu'
    initial_batch_size: int

    def set_save_config_params(self,):
        super().set_save_config_params()
        config_dict_ = asdict(self)
        for key in ['src_path', 'dst_dir_name', 'dst_file_name']:
            del config_dict_[key]
        self.config_dict.update(config_dict_)

    def __post_init__(self):
        super().__post_init__()
        
        if self.device not in ('auto', 'cpu', 'cuda'):
            raise ValueError("device should be one of 'auto' or 'cpu'.")
        
@dataclass(kw_only=True)
class TranslatePipelineConfig(PipelineConfig):
    language_from: str
    language_into: str