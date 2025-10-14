from pyspark.sql import DataFrame, SparkSession

from service.producer.bronze import *
from service.utils.schema.reader import AvscReader
from service.utils.helper import get_producer
from service.utils.spark import get_deserialized_avro_stream_df, get_kafka_stream_df

def get_producer(topic_name):
    if topic_name == OrderStatusBronzeProducer.dst_topic:
        return OrderStatusBronzeProducer
    
    elif topic_name == ProductBronzeProducer.dst_topic:
        return ProductBronzeProducer
    
    elif topic_name == CustomerBronzeProducer.dst_topic:
        return CustomerBronzeProducer
    
    elif topic_name == SellerBronzeProducer.dst_topic:
        return SellerBronzeProducer
    
    elif topic_name == GeolocationBronzeProducer.dst_topic:
        return GeolocationBronzeProducer
    
    elif topic_name == EstimatedDeliberyDateBronzeProducer.dst_topic:
        return EstimatedDeliberyDateBronzeProducer
    
    elif topic_name == OrderItemBronzeProducer.dst_topic:
        return OrderItemBronzeProducer
    
    elif topic_name == PaymentBronzeProducer.dst_topic:
        return PaymentBronzeProducer
    
    elif topic_name == ReviewBronzeProducer.dst_topic:
        return ReviewBronzeProducer
    else:
        raise ValueError(f"Topic does not exsits: {topic_name}")

def read_stream_df(spark_session: SparkSession, topic_name: str) -> DataFrame:
    avsc_reader = AvscReader(producer_class.dst_topic)

    producer_class = get_producer(topic_name)
    kafka_stream_df = get_kafka_stream_df(spark_session, producer_class.dst_topic)
    return get_deserialized_avro_stream_df(kafka_stream_df, producer_class, avsc_reader)