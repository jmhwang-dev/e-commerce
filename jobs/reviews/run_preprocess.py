from utils import *
from preprocess.reviews import *

if __name__ == "__main__":
    clean_comments_config = PreprocessConfig(
        src_path=get_bronze_data_path(OlistFileName.ORDER_REVIEWS),
        dst_path=os.path.join(SILVER_DIR, 'clean_comments.tsv'),
        inplace=True
        )
    
    clean_comments_config.save()

    reviews = get_bronze_df(OlistFileName.ORDER_REVIEWS)
    reviews_title = reviews[['review_id', 'review_comment_title']].dropna().reset_index(drop=True)
    reviews_message = reviews[['review_id', 'review_comment_message']].dropna().reset_index(drop=True)

    clean_titles = clean_review_comment(reviews_title, 'review_comment_title')
    clean_messages = clean_review_comment(reviews_message, 'review_comment_message')
    clean_comments = pd.concat([clean_titles, clean_messages], axis=0)
    clean_comments.to_csv(clean_comments_config.dst_path, sep='\t', index=False)