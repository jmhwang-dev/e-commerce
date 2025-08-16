from confluent_kafka import DeserializingConsumer, KafkaError, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient

# SR_URL = "http://schema-registry:8081"
# TOPIC = "review"

# sr_client = SchemaRegistryClient({'url': SR_URL})
# avro_deserializer = AvroDeserializer(schema_str=None, schema_registry_client=sr_client)

# def create_consumer(group_id: str) -> DeserializingConsumer:
#     return DeserializingConsumer({
#         'bootstrap.servers': 'kafka1:9092',
#         'group.id': group_id,
#         'auto.offset.reset': 'earliest',
#         'enable.auto.commit': False,
#         'value.deserializer': avro_deserializer,
#     })



def create_translator():
    from transformers.models.auto.tokenization_auto import AutoTokenizer
    from transformers.pipelines import pipeline
    import torch

    checkpoint = "Unbabel/TowerInstruct-7B-v0.2"
    device = "auto" if torch.cuda.is_available() else "cpu"
    tokenizer = AutoTokenizer.from_pretrained(checkpoint)
    tokenizer.padding_side = "left"

    translator = pipeline(
        "text-generation",
        model=checkpoint,
        tokenizer=tokenizer,
        torch_dtype=torch.bfloat16,
        device_map=device
    )

    return translator

def get_prompt(text: str) -> str:
    language_from = "Portuguese"
    language_into = "English"
    
    return [
        [{
            "role": "user",
            "content": f"Translate the following text from {language_from} into {language_into}.\n{language_from}: {text}\n{language_into}:"
        }]
    ]