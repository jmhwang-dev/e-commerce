from postprocess.reviews import *
from utils.config import *
from pathlib import Path
import pandas as pd

if __name__ == "__main__":
    translation_df = load_texts_as_df()
    # translation_df.to_csv("trans.csv", index=False)

    config_path_reviews_cleaned = Path(PREPROCESS_CONFIGS_DIR) / "reviews_cleaned.yml"
    reviews_clenaned_path = PreprocessConfig.load(config_path_reviews_cleaned).dst_path
    reviews_cleaned_df = pd.read_csv(reviews_clenaned_path)
    titles_cleaned= reviews_cleaned_df[['review_id', 'review_comment_title']].dropna()
    comments_cleaned= reviews_cleaned_df[['review_id', 'review_comment_message']].dropna()

    eng_comment_title_df = pd.merge(titles_cleaned, translation_df[['por_cleaned', 'eng']], left_on="review_comment_title", right_on='por_cleaned', how='left')
    eng_comment_title_df.drop(columns=['review_comment_title'], inplace=True)
    eng_comment_message_df = pd.merge(comments_cleaned, translation_df[['por_cleaned', 'eng']], left_on="review_comment_message", right_on='por_cleaned', how='left')
    eng_comment_message_df.drop(columns=['review_comment_message'], inplace=True)

    dup = reviews_cleaned_df['review_comment_message'].value_counts()
    print(dup[dup > 1])

    print(comments_cleaned.shape)
    print(comments_cleaned.drop_duplicates().shape)
    exit()

    # test
    find_str = r"produto entregue como previsto. só achei o material da arara a desejar. material que não dar muita segurança. tive q tirar as rodas pois sairam do lugar. no entanto como improviso"
    test_df = translation_df[translation_df['por_cleaned'].str.contains(find_str)]
    print(test_df['por_cleaned'].values)

    origin_df = comments_cleaned[comments_cleaned['review_comment_message'].str.contains(find_str)]
    print(origin_df['review_comment_message'].values)

    merge_test = pd.merge(origin_df, test_df[['por_cleaned', 'eng']], left_on="review_comment_message", right_on='por_cleaned', how='left')
    print(merge_test[['review_id', 'review_comment_message', 'por_cleaned']])
    # exit()

    # TODO: 따옴표 제거, 이모지 제거, 문장 내 따옴표 여러개를 하나로 치환,
    save_path = os.path.join(POSTPROCESS_ARTIFACTS_DIR, 'reviews_title.csv')
    eng_comment_title_df.drop_duplicates(inplace=True)
    eng_comment_title_df.to_csv(save_path, index=False)

    # save_path = os.path.join(POSTPROCESS_ARTIFACTS_DIR, 'reviews_message.csv')
    # eng_comment_message_df.drop_duplicates(inplace=True)
    # eng_comment_message_df.to_csv(save_path, index=False)