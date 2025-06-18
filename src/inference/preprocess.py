import pandas as pd
from inference.config import PreprocessConfig
from common.paths import *
from pathlib import Path

def cleanse_text(config: PreprocessConfig) -> None:
    df = pd.read_csv(config.src)

    target_columns = ['review_comment_title', "review_comment_message"]
    reviews_with_content_df = df[target_columns].dropna(how='all').copy()

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

    reviews_with_content_df = reviews_with_content_df.dropna(how='all')
    reviews_with_content_df['review_id'] = df.loc[reviews_with_content_df.index, 'review_id']
    reviews_with_content_df = reviews_with_content_df[['review_id'] + target_columns]

    # ✅ 결과 저장 경로: 전처리 artifact 디렉토리
    dst_path = Path(ARTIFACT_INFERENCE_PREPROCESS_DIR) / Path(config.dst).name
    reviews_with_content_df.to_csv(dst_path, index=False)

    print(f"Before preprocessing: {df.shape}")
    print(f"After preprocessing: {reviews_with_content_df.shape}")
    print(f"Saved cleansed reviews to: {dst_path}")


def extract_text(config: PreprocessConfig):
    reviews_with_content_df = pd.read_csv(config.src)

    unique_title = reviews_with_content_df['review_comment_title'].dropna().drop_duplicates()
    unique_message = reviews_with_content_df['review_comment_message'].dropna().drop_duplicates()

    all_portuguese = pd.concat([unique_title, unique_message])
    all_portuguese = all_portuguese.dropna()
    all_portuguese = all_portuguese.sort_values(key=lambda x: x.str.len(), ascending=False)

    # ✅ 결과 저장 경로: 전처리 artifact 디렉토리
    dst_path = Path(ARTIFACT_INFERENCE_PREPROCESS_DIR) / Path(config.dst).name
    all_portuguese.to_csv(dst_path, index=False, header=False)

    print(f"Portuguese to translate: {dst_path}")


if __name__ == "__main__":
    config_cleanse = PreprocessConfig(
        src=os.path.join(BRONZE_DIR, "olist_order_reviews_dataset.csv"),
        dst=os.path.join(ARTIFACT_INFERENCE_PREPROCESS_DIR, "olist_order_reviews_dataset.csv"),
        inplace=False
    )
    config_cleanse.save()
    cleanse_text(config_cleanse)

    config_extract = PreprocessConfig(
        src=config_cleanse.dst,
        dst=os.path.join(ARTIFACT_INFERENCE_PREPROCESS_DIR, "all_portuguese.txt"),
        inplace=False
    )
    config_extract.save()
    extract_text(config_extract)
