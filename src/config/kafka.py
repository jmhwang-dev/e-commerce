from dotenv import load_dotenv
from pathlib import Path
from typing import Iterator
from enum import Enum
import os

load_dotenv('./configs/kafka/.env')
BOOTSTRAP_SERVERS_EXTERNAL = os.getenv("BOOTSTRAP_SERVERS_EXTERNAL", "localhost:19092,localhost:19094,localhost:19096").split(",")
BOOTSTRAP_SERVERS_INTERNAL = os.getenv("BOOTSTRAP_SERVERS_INTERNAL", "kafka1:9092,kafka2:19092,kafka3:9092")
DATASET_DIR = Path(os.getenv("DATASET_DIR", "./downloads/olist_redefined"))

class IngestionType(Enum):
    CDC = 'cdc'
    STREAM = 'stream'

class Topic:
    # CDC
    ORDER_ITEM = 'order_item'
    PRODUCT = 'product'
    CUSTOMER = 'customer'
    SELLER = 'seller'
    GEOLOCATION = 'geolocation'

    # stream
    ORDER_STATUS = 'order_status'
    PAYMENT = 'payment'
    ESTIMATED_DELIVERY_DATE = 'estimated_delivery_date'
    REVIEW = 'review'

    # inference
    PREPROCESSED_REVIEW = 'inferenced_review'
    INFERENCED_REVIEW = 'inferenced_review'

    @classmethod
    def __iter__(cls) -> Iterator[str]:
        for attr_name, attr_value in vars(cls).items():
            if not attr_name.startswith('__'):
                yield attr_value