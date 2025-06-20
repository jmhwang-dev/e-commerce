from common import *
from preprocess.inference.reviews import *

if __name__ == "__main__":
    config_cleanse = PreprocessConfig(
        src_path=os.path.join(BRONZE_DIR, "olist_order_reviews_dataset.csv"),
        dst_path=os.path.join(ARTIFACTS_PREPROCESS_DIR, "reviews_cleaned.csv"),
        inplace=True
    )
    config_cleanse.save()
    clean_text(config_cleanse)

    config_extract = PreprocessConfig(
        src_path=config_cleanse.dst_path,
        dst_path=os.path.join(ARTIFACTS_PREPROCESS_DIR, "reviews_textonly.txt"),
        inplace=True
    )
    config_extract.save()
    extract_text(config_extract)