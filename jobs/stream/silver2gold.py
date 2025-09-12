from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from service.stream.topic import SilverTopic
from service.utils.spark import get_spark_session, get_kafka_stream_df, get_deserialized_stream_df, start_console_stream
from service.utils.schema.reader import AvscReader
from service.stream.helper import *
from service.utils.iceberg import append_or_create_table

SRC_TOPIC_NAMES = [SilverTopic.STREAM_PAYMENT]
DST_TABLE_IDENTIFIER = 'warehousedev.gold.realtime_sales_overall'

def aggregate_realtime_sales_summary(micro_batch_df:DataFrame, batch_id: int):
    micro_batch_df.cache()
    print(f"Processing Batch ID: {batch_id}")

    topic_dict = {}
    for src_topic_name in SRC_TOPIC_NAMES:
        try:
            df = micro_batch_df.filter(col("topic") == src_topic_name)
            avsc_reader = AvscReader(src_topic_name)
            deserialized_df = get_deserialized_stream_df(df, avsc_reader.schema_str)

            if src_topic_name == SilverTopic.STREAM_PAYMENT:
                # root
                # |-- order_id: string (nullable = true)
                # |-- payment_sequential: integer (nullable = true)
                # |-- timestamp: timestamp (nullable = true)
                # |-- payment_value: decimal(18,2) (nullable = true)
                # |-- payment_type: string (nullable = true)
                # |-- payment_installments: integer (nullable = true)
                # |-- customer_id: string (nullable = true)

                deserialized_df_with_types = deserialized_df.withColumn(
                    "payment_value",
                    F.col("payment_value").cast(DecimalType(18, 2))
                )

                # 1. 'timestamp' 컬럼으로 워터마크 설정 (스키마에 맞게 컬럼명 수정)
                watermarked_df = deserialized_df_with_types.withWatermark("timestamp", "20 seconds")

                agg_df = watermarked_df.groupBy(
                    F.window(F.col("timestamp"), "5 minutes")  # 5분 단위 텀블링 윈도우
                ).agg(
                    # 2. .astype(int) -> .cast(LongType()) 으로 수정 (count 결과는 클 수 있으므로 LongType 권장)
                    F.countDistinct("order_id").cast(LongType()).alias("purchase_count"),
                    
                    # 3. sum -> F.sum 으로 수정
                    F.sum("payment_value").alias("total_sales")
                )
                # agg_df.show(truncate=False)
                agg_df.write \
                    .format("iceberg") \
                    .mode("append") \
                    .save(DST_TABLE_IDENTIFIER)

                print('----updated-----')
                
        except Exception as e:
            print(f"{src_topic_name} in batch {batch_id}: {e}")
        

if __name__ == "__main__":
    spark_session = get_spark_session("Aggregate realtime sales summary")
    src_stream_df = get_kafka_stream_df(spark_session, SRC_TOPIC_NAMES)

    spark_session.sql(f"""
        DROP TABLE IF EXISTS {DST_TABLE_IDENTIFIER}
    """)

    spark_session.sql(f"""
        CREATE TABLE IF NOT EXISTS {DST_TABLE_IDENTIFIER} (
            window STRUCT<start: TIMESTAMP, end: TIMESTAMP>,
            purchase_count LONG,
            total_sales DECIMAL(38, 2)
        )
        USING iceberg
        PARTITIONED BY (hours(window.start))
    """)
    
    
    src_stream_df.createOrReplaceTempView('stream_payment')    
    
    query = src_stream_df.writeStream \
        .foreachBatch(aggregate_realtime_sales_summary) \
        .queryName("aggregate_sales") \
        .option("checkpointLocation", f"s3a://warehousedev/gold/checkpoints") \
        .trigger(processingTime="10 seconds") \
        .start()
        
    query.awaitTermination()