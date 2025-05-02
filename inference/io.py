from typing import List
import yaml
from config import *

def load_config(src_dir_name, src_config_name) -> BaseConfig:
    config_path = os.path.join(ARTIFACT_DIR, src_dir_name, src_config_name)
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config

def load_dataset(dataset_path) -> List[str]:
    dataset = []
    with open(dataset_path, 'r') as f:
        for portuguese in f.readlines():
            dataset.append(portuguese.strip())
    return dataset