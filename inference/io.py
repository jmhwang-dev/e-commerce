
from typing import List, Dict, Union
import pandas as pd
import yaml
from config import *

def load_config(src_dir_name, src_config_name) -> BaseConfig:
    config_path = os.path.join(ARTIFACT_DIR, src_dir_name, src_config_name)
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config

def load_dataset(dataset_path:str) -> List[str]:
    dataset = []
    with open(dataset_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    for portuguese in lines:
        dataset.append(portuguese.strip())
    return dataset

def save_sentiment(dst_path: str, score: List[Dict[str, Union[float, str]]]) -> None:
    df_new = pd.DataFrame(score)
    try:
        existing_df = pd.read_csv(dst_path)
        df = pd.concat([existing_df, df_new], ignore_index=True)
    except FileNotFoundError:
        df = df_new
    finally:
        df.to_csv(dst_path, index=False)