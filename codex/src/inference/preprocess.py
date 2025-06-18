import pandas as pd
from inference.config import *

def cleanse_text(config: BaseConfig) -> None:
    df = pd.read_csv(config.src_path)

    target_columns = ['review_comment_title', "review_comment_message"]
    reviews_with_content_df = df[target_columns].dropna(how='all')

    for col in target_columns:
        # 문자열 처리: 소문자화, 특수문자 정리 등
        reviews_with_content_df[col] = (
            reviews_with_content_df[col]
            .str.lower()
            .str.replace(r'[\r\n]', ' ', regex=True)      # 줄바꿈 문자 → 공백
            .str.replace(r'\s+', ' ', regex=True)         # 연속된 공백 → 하나로
            .str.replace(r'\s+\.', '.', regex=True)       # 공백 + 마침표 → 마침표
            .str.replace(r'[!*]+', '.', regex=True)       # 느낌표/별표 → 마침표
            .str.replace(r'\.{2,}', '.', regex=True)      # 마침표 여러 개 → 하나로
            .str.replace(r'[?]+', '?', regex=True)        # 물음표 여러 개 → 하나로
            .str.strip()                                  # 양쪽 공백 제거
        )

        # 의미 없는 값들(None 처리): 공백/마침표/쉼표만 있는 경우 등
        reviews_with_content_df[col] = reviews_with_content_df[col].replace(
            r'^[\s.,]+$', None, regex=True
        )
        reviews_with_content_df[col] = reviews_with_content_df[col].replace(
            r'^\.$', None, regex=True
        )
        reviews_with_content_df[col] = reviews_with_content_df[col].replace(
            r'^$', None, regex=True
        )

    reviews_with_content_df = reviews_with_content_df[target_columns].dropna(how='all')
    reviews_with_content_df['review_id'] = df.loc[reviews_with_content_df.index, 'review_id']
    reviews_with_content_df = reviews_with_content_df[['review_id'] + target_columns]
    
    reviews_with_content_df.to_csv(config.dst_path, index=False)

    print(f"Before preprocessing: {df.shape}")
    print(f"After preprocessing: {reviews_with_content_df.shape}")

def extract_text(config:BaseConfig):
    reviews_with_content_df = pd.read_csv(config.src_path)
    unique_title = reviews_with_content_df['review_comment_title'].dropna().drop_duplicates()
    unique_message = reviews_with_content_df['review_comment_message'].dropna().drop_duplicates()
    
    all_portuguese = pd.concat([unique_title, unique_message])
    all_portuguese = all_portuguese.dropna()
    all_portuguese = all_portuguese.sort_values(key=lambda x: x.str.len(), ascending=False)

    all_portuguese.to_csv(config.dst_path, index=False, header=False)
    print(f"Portuguess to translate: {config.dst_path}")

if __name__=="__main__":
    # TO RUN: python -m inference.preprocess

    config_cleanse = BaseConfig(
        src_path="./artifact/medallion/bronze/olist/olist_order_reviews_dataset.csv",
        dst_dir_name='preprocess',
        dst_file_name="order_reviews.csv"
    )
    cleanse_text(config_cleanse)
    config_cleanse.save()

    config_extract = BaseConfig(
        src_path=config_cleanse.dst_path,
        dst_dir_name='preprocess',
        dst_file_name="all_portuguess.txt"
    )

    extract_text(config_extract)
    config_extract.save()