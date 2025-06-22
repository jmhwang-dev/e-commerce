import os
import json
import pandas as pd
from enum import Enum
from .paths import *
from typing import List

class OlistFileName(Enum):
    CUSTOMERS = "customers"
    GEOLOCATION = "geolocation"
    ORDER_ITEMS = "order_items"
    ORDER_PAYMENTS = "order_payments"
    ORDER_REVIEWS = "order_reviews"
    ORDERS = "orders"
    PRODUCTS = "products"
    SELLERS = "sellers"
    CATEGORY = "product_category_name_translation"

def get_bronze_df(file_name: OlistFileName) -> pd.DataFrame:
    with open(os.path.join(BRONZE_DIR, 'eda', 'pandas', 'paths.json'), 'r') as f:
        paths_dict = json.load(f)

    df = pd.read_csv(paths_dict[file_name.value])
    print(df.shape)
    df.drop_duplicates(inplace=True)
    return df

def get_silver_df(file_name: OlistFileName) -> pd.DataFrame:    
    path = os.path.join(SILVER_DIR, f'{file_name.value}.csv')
    if not os.path.exists(path):
        raise FileNotFoundError(f"Check path: {path}")

    df = pd.read_csv(path)
    return df

def load_texts(dataset_path:str) -> List[str]:
    dataset = []
    with open(dataset_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    for portuguese in lines:
        dataset.append(portuguese.strip())
    return dataset