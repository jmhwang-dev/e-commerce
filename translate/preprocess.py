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
        reviews_with_content_df[col] = reviews_with_content_df[col].replace(
            r'^$', None, regex=True
        )

    reviews_with_content_df = reviews_with_content_df[target_columns].dropna(how='all')
    reviews_with_content_df['review_id'] = df.loc[reviews_with_content_df.index, 'review_id']
    reviews_with_content_df = reviews_with_content_df[['review_id'] + target_columns]
    
    os.makedirs(PREPROCESSED_RESULT_DST_DIR, exist_ok=True)
    reviews_with_content_df.to_csv(PREPROCESSED_RESULT_DST_PATH, index=False)

    print(f"Before preprocessing: {df.shape}")
    print(f"After preprocessing: {reviews_with_content_df.shape}")

    unique_title = reviews_with_content_df['review_comment_title'].dropna().drop_duplicates()
    unique_message = reviews_with_content_df['review_comment_message'].dropna().drop_duplicates()
    
    all_portuguese = pd.concat([unique_title, unique_message])
    all_portuguese = all_portuguese.dropna()
    all_portuguese = all_portuguese.sort_values(key=lambda x: x.str.len(), ascending=False)

    all_portuguese_save_path = os.path.join(PREPROCESSED_RESULT_DST_DIR, 'all_portuguess.txt')
    all_portuguese.to_csv(all_portuguese_save_path, index=False, header=False)
    print(f"Portuguess to translate: {all_portuguese_save_path}")