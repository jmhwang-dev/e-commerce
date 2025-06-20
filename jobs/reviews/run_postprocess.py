from postprocess.inference.reviews import *
from common.config import *
from pathlib import Path
import pandas as pd

if __name__ == "__main__":
    config_path_reviews_textonly = Path(CONFIGS_PREPROCESS_DIR) / "reviews_textonly.yml"
    config_path_reviews_cleaned = Path(CONFIGS_PREPROCESS_DIR) / "reviews_cleaned.yml"

    config_path_eng2kor_1 = Path(CONFIGS_INFERENCE_DIR) / "eng2kor_1.yml"
    config_path_eng2kor_2 = Path(CONFIGS_INFERENCE_DIR) / "eng2kor_2.yml"
    config_path_por2eng_1 = Path(CONFIGS_INFERENCE_DIR) / "por2eng_1.yml"
    config_path_por2eng_2 = Path(CONFIGS_INFERENCE_DIR) / "por2eng_2.yml"

    reviews_textonly_path = PreprocessConfig.load(config_path_reviews_textonly).dst_path
    reviews_clenaned_path = PreprocessConfig.load(config_path_reviews_cleaned).dst_path

    eng2kor_1_path = TranslatePipelineConfig.load(config_path_eng2kor_1).dst_path
    eng2kor_2_path = TranslatePipelineConfig.load(config_path_eng2kor_2).dst_path
    por2eng_1_path = TranslatePipelineConfig.load(config_path_por2eng_1).dst_path
    por2eng_2_path = TranslatePipelineConfig.load(config_path_por2eng_2).dst_path

    reviews_textonly_df = pd.read_csv(reviews_textonly_path, header=None)
    reviews_textonly_df.columns = ['por']
    eng2kor_df = concat_results([eng2kor_1_path, eng2kor_2_path], col_name='kor')
    por2eng_df = concat_results([por2eng_1_path, por2eng_2_path], col_name='eng')
    translation_df = pd.concat([reviews_textonly_df, por2eng_df, eng2kor_df], axis=1)
    translation_df = remove_special_chars(translation_df)

    translation_df = translation_df[['por', 'eng', 'kor', 'max_sentimental']]
    translation_df.drop_duplicates(inplace=True)

    reviews_cleaned = pd.read_csv(reviews_clenaned_path)
    titles_cleaned= reviews_cleaned[['review_id', 'review_comment_title']].dropna()
    comments_cleaned= reviews_cleaned[['review_id', 'review_comment_message']].dropna()

    eng_comment_title_df = pd.merge(titles_cleaned, translation_df, left_on="review_comment_title", right_on='por', how='left')
    eng_comment_title_df.drop(columns=['review_comment_title'], inplace=True)
    print(eng_comment_title_df.head())
    eng_comment_message_df = pd.merge(comments_cleaned, translation_df, left_on="review_comment_message", right_on='por', how='left')
    eng_comment_message_df.drop(columns=['review_comment_message'], inplace=True)

    # save_path = os.path.join(ARTIFACT_ROOT_DIR, 'eng_kor_title_with_senti.csv')
    # eng_comment_title_df.drop_duplicates(inplace=True)
    # eng_comment_title_df.to_csv(save_path, index=False)
    # print(save_path)

    # save_path = os.path.join(ARTIFACT_ROOT_DIR, 'eng_kor_message_with_senti.csv')
    # eng_comment_message_df.drop_duplicates(inplace=True)
    # eng_comment_message_df.to_csv(save_path, index=False)
    # print(save_path)