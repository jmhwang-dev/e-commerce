import pandas as pd
# import numpy as np
import os
from config import *

if __name__=="__main__":
    df = pd.read_csv("./downloads/olist/olist_order_reviews_dataset.csv")

    target_columns = ['review_comment_title', "review_comment_message"]
    reviews_with_content_df = df[target_columns].dropna(how='all')
    # target_df = df.loc[reviews_with_content_df.index, ['review_id'] + target_columns]

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

    reviews_with_content_df = reviews_with_content_df[target_columns].dropna(how='all')
    reviews_with_content_df['review_id'] = df.loc[reviews_with_content_df.index, 'review_id']

    reviews_with_content_df = reviews_with_content_df[['review_id'] + target_columns]
    reviews_with_content_df.rename(columns={"review_comment_title": "title", 'review_comment_message': "comment"}, inplace=True)

    os.makedirs(PREPROCESSED_RESULT_DST_DIR, exist_ok=True)
    reviews_with_content_df.to_csv(PREPROCESSED_RESULT_DST_PATH, index=False)

    print(f"Before preprocessing: {df.shape}")
    print(f"After preprocessing: {reviews_with_content_df.shape}")

    for col in ['title', 'comment']:
        unique_values = reviews_with_content_df[col].dropna().unique()
        unique_values = sorted(unique_values, key=lambda x: len(x), reverse=True)
        print(f"# of unique {col}: {len(unique_values)}")
        file_name = f'unique_{col}.txt'
        save_path = os.path.join(PREPROCESSED_RESULT_DST_DIR, file_name)
        with open(save_path, 'w', encoding='utf-8') as f:
            for title in unique_values:
                f.write(f"{title}\n")