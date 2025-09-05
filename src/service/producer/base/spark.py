from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws
from service.producer.base.common import *

from config.kafka import BOOTSTRAP_SERVERS_INTERNAL


class SparkProducer(BaseProducer):
    @classmethod
    def generate_message(cls, data: DataFrame) -> DataFrame:
        """Generate Kafka message by adding a message key column."""
        return data.withColumn(cls.message_key_col, concat_ws('-', *[col(c).cast("string") for c in cls.pk_column]))

    @classmethod
    def publish(cls, serialized_df: DataFrame):
        """
        직렬화가 완료된 DataFrame (key, value 컬럼 포함)을
        Spark의 내장 Kafka Sink를 사용하여 발행합니다.
        """
        if serialized_df.isEmpty():
            return
            
        (serialized_df
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS_INTERNAL)
            .option("topic", cls.topic) # .value 추가 (Enum 객체이므로)
            .save())


    # @classmethod
    # def publish(cls, partition: Iterator[Row]):
    #     """
    #     Process a single partition and send data to Kafka or DLQ on failure.
    #     Each row is processed individually with exception handling.
    #     """
    #     cls.init_producer(use_internal=True) # For thread-safety.
    #     for row in partition:
    #         try:
    #             message = row.asDict()
    #             message_key = message.get(cls.message_key_col, "invalid_key")
    #             message_value = {k: v for k, v in message.items() if k != cls.message_key_col}
    #             cls.producer.produce(cls.topic, key=message_key, value=message_value)
    #         except (SerializationError, KeyError, TypeError):
    #             cls.dlq_producer.produce(cls.topic_dlq, key=message_key, value=json.dumps(message_value))

    #     cls.producer.flush()
    #     cls.dlq_producer.flush()

    # @classmethod
    # def write_to_kafka(cls, batch_df: DataFrame, batch_id: int) -> None:
    #     """Write a batch DataFrame to Kafka using foreachPartition."""
    #     if batch_df.rdd.isEmpty():
    #         return
    #     batch_df = cls.generate_message(batch_df)
    #     batch_df.rdd.foreachPartition(cls.process_partition)

    # @classmethod
    # def publish(cls, spark_df: DataFrame) -> None:
    #     query = spark_df.writeStream \
    #                    .foreachBatch(cls.write_to_kafka) \
    #                    .trigger(processingTime="15 seconds") \
    #                    .start()
    #     return query