import pandas as pd
import os
import time
from pprint import pformat

from producer.init import *
from producer.topic import *

PRODUCER = get_producer()

def simulate_stream(process_type, topic, key_field, file_name):
    df = pd.read_csv(DATASET_DIR / process_type / file_name, sep='\t')
    
    for _, row in df.iterrows():
        message_dict = row.to_dict()
        message_key = message_dict.get(key_field)
        
        # Producer send
        PRODUCER.send(
            topic, 
            key=message_key, 
            value=message_dict
        )
        print(f'Published: \n{pformat(message_dict)}')
        time.sleep(1)
        # break  # 테스트용이면 유지, 전체 데이터 처리하려면 제거
    
    PRODUCER.flush()