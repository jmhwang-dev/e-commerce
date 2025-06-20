from postprocess.inference.reviews import *
from common.config import *
from pathlib import Path
import pandas as pd

if __name__ == "__main__":
    olist_order_reviews_dataset_config_path = Path(ARTIFACT_INFERENCE_PREPROCESS_DIR) / "olist_order_reviews_dataset_config.yml"
    preprocessed_reviews_config = PreprocessConfig.load(olist_order_reviews_dataset_config_path)
    p2e_config = TranslatePipelineConfig.load(olist_order_reviews_dataset_config_path)
    e2k_config = TranslatePipelineConfig.load(olist_order_reviews_dataset_config_path)

    paths_to_merge = [
        preprocessed_reviews_config.dst_path,
    ]
    # print(preprocessed_reviews.head())