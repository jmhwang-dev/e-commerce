from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv('./configs/kafka/.env')
BOOTSTRAP_SERVERS_EXTERNAL = os.getenv("BOOTSTRAP_SERVERS_EXTERNAL", "localhost:19092,localhost:19094,localhost:19096")
BOOTSTRAP_SERVERS_INTERNAL = os.getenv("BOOTSTRAP_SERVERS_INTERNAL", "kafka1:9092,kafka2:19092,kafka3:9092")
DATASET_DIR = Path(os.getenv("DATASET_DIR", "./downloads/olist_redefined"))