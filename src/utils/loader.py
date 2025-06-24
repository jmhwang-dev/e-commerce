import os
import json
import pandas as pd
from enum import Enum
from .paths import *
from typing import List, Union
from pathlib import Path

class BronzeDataName(Enum):
    CUSTOMERS = "customers"
    GEOLOCATION = "geolocation"
    ORDER_ITEMS = "order_items"
    ORDER_PAYMENTS = "order_payments"
    ORDER_REVIEWS = "order_reviews"
    ORDERS = "orders"
    PRODUCTS = "products"
    SELLERS = "sellers"
    CATEGORY = "product_category_name_translation"

class SilverDataName(Enum):
    CLEAN_REVIEWS = "clean_comments.tsv"
    CLEAN_REVIEWS_TEXT_ONLY = "clean_comments_text_only.tsv"

def get_bronze_dataset(file: Union[BronzeDataName, Path]) -> Union[pd.DataFrame, Path]:
    if isinstance(file, BronzeDataName):
        with open(os.path.join(METADATA_ARTIFACT_DIR, 'bronze_paths.json'), 'r') as f:
            paths_dict = json.load(f)
        path = paths_dict[file.value]
    elif isinstance(file, Path):
        path = file
    else:
        raise TypeError(f"Unsupported type: {type(file)}")

    dataset = load_file(path)
    return dataset, path

def get_silver_dataset(file_name: SilverDataName) -> pd.DataFrame:    
    path = Path(SILVER_DIR) / file_name.value
    
    if not os.path.exists(path):
        raise FileNotFoundError(f"Check path: {path}")

    return load_file(path)

def load_file(path: Union[str, Path]) -> Union[pd.DataFrame, List[str]]:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    suffix = path.suffix.lower()

    if suffix == '.tsv':
        return pd.read_csv(path, sep='\t').drop_duplicates()
    elif suffix == '.csv':
        return pd.read_csv(path).drop_duplicates()
    elif suffix == '.txt':
        with path.open('r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip()]
    else:
        raise ValueError(f"Unsupported file extension: {suffix}")