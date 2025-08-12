import pandas as pd

from service.producer.raw_message import *

def create_df(message_key: str, message_value: dict, target_cols: List[str]) -> pd.DataFrame:
    data = {}
    for col in target_cols:
        if col == 'review_id':
            data[col] = message_key
        else:
            data[col] = message_value[col]

    return pd.DataFrame([data]) 