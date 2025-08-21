from service.consumer.inference import *
from service.common.schema import *
from service.common.topic import *
from service.utils.kafka import *
from service.producer.silver import ReviewInferedSilverProducer

if __name__=="__main__":
    consumer = get_confluent_kafka_consumer('inference', [BronzeToSilverTopic.REVIEW_CLEAN_COMMENT], use_internal=True)
    wait_for_partition_assignment(consumer)

    translator = get_translator()
    analyzer = get_sentiment_analyzer()

    while True:
        messages = get_messages(consumer, num_message=5)
        message_df = message2dataframe(messages)

        prompts = message_df['portuguess'].apply(get_prompts).to_list()
        por2eng_df = translate(translator, prompts)

        eng_text_list = por2eng_df['eng'].to_list()
        senti_df = analyze(analyzer, eng_text_list)

        inference_result_df = pd.concat([message_df, por2eng_df, senti_df], axis=1).drop(columns=['portuguess'])
        ReviewInferedSilverProducer.publish(inference_result_df, use_internal=True)
