from utils import *
from preprocess.reviews import *

if __name__ == "__main__":

    reviews = get_bronze_df(OlistFileName.ORDER_REVIEWS)
    print(reviews)
    # config_cleanse = PreprocessConfig(
    #     src_path=os.path.join(BRONZE_DIR, "olist_order_reviews_dataset.csv"),
    #     dst_path=os.path.join(PREPROCESS_ARTIFACTS_DIR, "reviews_cleaned.csv"),
    #     inplace=True
    # )
    # config_cleanse.save()
    # clean_text(config_cleanse)

    # config_extract = PreprocessConfig(
    #     src_path=config_cleanse.dst_path,
    #     dst_path=os.path.join(PREPROCESS_ARTIFACTS_DIR, "reviews_textonly.txt"),
    #     inplace=True
    # )
    # config_extract.save()
    # extract_text(config_extract)