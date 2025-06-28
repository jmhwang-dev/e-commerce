from postprocess import *

if __name__=="__main__":
    sent_config_file_name = "sentiment_20250628_160638.yml"
    trans_config_file_name = "por2eng_20250626_172543.yml"
    dst_path = Path(SILVER_DIR) / "eng_reviews_senti.tsv"

    gather_df = gather_inference(sent_config_file_name, trans_config_file_name)

    if not is_conflict(gather_df):
        clean_comments_df, _ = get_dataset(SilverDataName.CLEAN_REVIEWS)
        result = pd.merge(clean_comments_df, gather_df, left_on='comment', right_on='comment', how='left')
        result.to_csv(dst_path, sep='\t', index=False)