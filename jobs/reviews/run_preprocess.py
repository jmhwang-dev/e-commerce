from utils import *
from preprocess.reviews import *

if __name__ == "__main__":
    dataset, path = get_dataset(BronzeDataName.ORDER_REVIEWS)

    clean_comments_config = PreprocessConfig(
        src_path=path,
        dst_path=os.path.join(SILVER_DIR, SilverDataName.CLEAN_REVIEWS.value),
        inplace=True
    )
    clean_comments_config.save()

    preprocessor = ReviewPreprocessor(
        dataset=dataset,
        target_cols=['review_id', 'review_comment_title', 'review_comment_message'],
        manual_fix_json_path=os.path.join(PREPROCESS_ARTIFACTS_DIR, 'manual_fix_reviews.json')
    )

    fixed_df = preprocessor.run(clean_comments_config.dst_path)

    # text only 저장
    clean_comments_textonly_config = PreprocessConfig(
        src_path=clean_comments_config.dst_path,
        dst_path=os.path.join(SILVER_DIR, SilverDataName.CLEAN_REVIEWS_TEXT_ONLY.value),
        inplace=True
    )
    clean_comments_textonly_config.save()

    fixed_df[preprocessor.value_column_name].drop_duplicates().to_csv(
        clean_comments_textonly_config.dst_path,
        sep='\t',
        index=False
    )

