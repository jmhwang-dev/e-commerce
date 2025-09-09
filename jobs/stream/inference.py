from service.stream.inference import *
from service.stream.topic import SilverTopic
from service.utils.kafka import *
from service.producer.inference import ReviewConflictSentimentSilverProducer, ReviewConsistentSentimentSilverProducer

if __name__=="__main__":
    translator = get_translator()
    analyzer = get_sentiment_analyzer()

    consumer = get_confluent_kafka_consumer('inference', [SilverTopic.REVIEW_CLEAN_COMMENT], use_internal=True)
    wait_for_partition_assignment(consumer)

    while True:
        # TODO: apply dynamic modulation of batch size
        messages = fetch_batch(consumer)
        message_df = message2dataframe(messages)

        prompts = message_df['portuguess'].apply(get_prompts).to_list()
        por2eng_df = translate(translator, prompts)

        eng_text_list = por2eng_df['eng'].to_list()
        senti_df = analyze(analyzer, eng_text_list)

        inference_result_df = pd.concat([message_df, por2eng_df, senti_df], axis=1).drop(columns=['portuguess'])
        conflict_df, consistent_df = split_conflict_main_sentiment(inference_result_df)
        ReviewConflictSentimentSilverProducer.publish(conflict_df, use_internal=True)
        ReviewConsistentSentimentSilverProducer.publish(consistent_df, use_internal=True)