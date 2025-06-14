import os
import json
import pandas as pd
from enum import Enum

ARTIFACT_DIR = "../../artifact"
PREPROCESSED_DIR = "../../artifact/preprocess"


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
    if not os.path.exists(ARTIFACT_DIR):
        raise FileNotFoundError(f"Check path: {ARTIFACT_DIR}")

    with open(os.path.join(ARTIFACT_DIR, 'eda', 'pandas', 'paths.json'), 'r') as f:
        paths_dict = json.load(f)

    df = pd.read_csv(paths_dict[file_name.value])
    df.drop_duplicates(inplace=True)
    return df

def get_preprocessed_df(file_name: OlistFileName) -> pd.DataFrame:
    if not os.path.exists(PREPROCESSED_DIR):
        raise FileNotFoundError(f"Check path: {PREPROCESSED_DIR}")
    
    path = os.path.join(PREPROCESSED_DIR, f'{file_name}.csv')
    if not os.path.exists(path):
        raise FileNotFoundError(f"Check path: {path}")

    df = pd.read_csv(path)
    return df
