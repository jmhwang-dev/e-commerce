from service.init.kafka import *
from service.consumer.review import *
from ecommerce.pipelines.bronze2silver.reviews import *

if __name__=="__main__":
    try:
        # # topic_list = list(Topic().__iter__())

        # message_key = "dc62f1e20d7f280e54066b6a92841086"
        # message_value = {
        #     'review_creation_date': '2016-10-28 00:00:00',
        #     'review_answer_timestamp': '2016-10-29 11:29:53',
        #     'order_id': 'be5bc2f0da14d8071e2d45451ad119d9',
        #     'review_score': 4,
        #     'review_comment_title': None,
        #     'review_comment_message': 'Tudo certo...'}

        # # 원본 bronze 저장
        # target_cols = ['review_id', 'review_comment_title', 'review_comment_message']
        # df = create_df(message_key, message_value, target_cols)

        # # 전처리: review_comment_title, review_comment_message
        # melted_review = PortuguessPreprocessor.melt_reviews(df)
        # clean_review = PortuguessPreprocessor.clean_review_comment(melted_review)

        # # 전처리한 데이터 silver로 저장
        # # 전처리한 데이터 review_inference 토픽으로 발행

        consumer = get_consumer(['review'])
        for message in consumer:
            print(message)
            pass
        
    except TimeoutError as e:
        print(str(e))
    except KeyboardInterrupt as e:
        print(str(e))
    # finally:
    #     consumer.close()