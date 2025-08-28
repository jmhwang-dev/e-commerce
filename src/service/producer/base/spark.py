from typing import Iterator
import json

from confluent_kafka.serialization import SerializationError
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col, concat_ws
from service.producer.base.common import *

class SparkProducer(BaseProducer):
    @classmethod
    def generate_message(cls, data: DataFrame) -> DataFrame:
        """Generate Kafka message by adding a message key column."""
        return data.withColumn(cls.message_key_col, concat_ws('-', *[col(c).cast("string") for c in cls.pk_column]))

    @classmethod
    def process_partition(cls, partition: Iterator[Row]):
        """
        Process a single partition and send data to Kafka or DLQ on failure.
        Each row is processed individually with exception handling.
        """
        cls.init_producer(use_internal=True) # For thread-safety.
        for row in partition:
            try:
                message = row.asDict()
                message_key = message.get(cls.message_key_col, "invalid_key")
                message_value = {k: v for k, v in message.items() if k != cls.message_key_col}
                cls.main_producer.produce(cls.topic, key=message_key, value=message_value)
            except (SerializationError, KeyError, TypeError):
                cls.dlq_producer.produce(cls.topic_dlq, key=message_key, value=json.dumps(message_value))

        cls.main_producer.flush()
        cls.dlq_producer.flush()

    @classmethod
    def write_to_kafka(cls, batch_df: DataFrame, batch_id: int) -> None:
        """Write a batch DataFrame to Kafka using foreachPartition."""
        if batch_df.rdd.isEmpty():
            return
        batch_df = cls.generate_message(batch_df)
        batch_df.rdd.foreachPartition(cls.process_partition)

    @classmethod
    def publish(cls, spark_df: DataFrame) -> None:
        query = spark_df.writeStream \
                       .foreachBatch(cls.write_to_kafka) \
                       .trigger(processingTime="15 seconds") \
                       .start()
        return query