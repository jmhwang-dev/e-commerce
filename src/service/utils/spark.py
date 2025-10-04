from typing import Iterable, Union

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.functions import col, expr, from_json, concat_ws, struct
from pyspark.sql.avro.functions import from_avro

from config.spark import *
from config.kafka import *

from service.producer.silver import SparkProducer

def get_serialized_df(serializer_udfs: dict[str, ], transformed_df: DataFrame, producer_class: SparkProducer):
    serializer_udf = serializer_udfs.get(producer_class.dst_topic)
    if not serializer_udf:
        raise ValueError(f"Warning: Serializer UDF for destination topic '{producer_class.dst_topic}' not found. Skipping.")
        
    df_to_publish = transformed_df.select(
        concat_ws("-", *[col(c).cast("string") for c in producer_class.key_column]).alias("key"),
        # struct('*')를 사용하여 데이터프레임의 모든 컬럼을 value_struct로 만듬
        struct(*transformed_df.columns).alias("value_struct")
    )

    return df_to_publish.withColumn(
        "value", serializer_udf(col("value_struct"))
    )

def get_spark_session(app_name: str=None, dev=False) -> SparkSession:
    """
    confs

    - spark.conf.set("spark.sql.shuffle.partitions", "50")
        : 셔플 작업의 파티션 수. 스트리밍 병렬 처리 성능에 영향.
    
    - spark.conf.set("spark.sql.streaming.commitIntervalMs", "5000")
        : Kafka 오프셋 커밋 주기(ms). 빠른 커밋 vs 안정성 조절.

    - spark.conf.set("spark.hadoop.fs.s3a.multipart.size", "268435456") // 256MB
        : minIO에 업로드 시 멀티파트 크기. 큰 데이터 전송 성능 최적화.

    """
        # .set("spark.eventLog.enabled", "true") \
        # .set("spark.eventLog.dir", "file:///opt/spark/logs/") \
        # .set("spark.history.fs.logDirectory", "file:///opt/spark/logs/") \
        # .set("spark.history.ui.port", "18080") \
        # .set("spark.metrics.appStatusSource.enabled", "true") \
    dev_conf = SparkConf().setAppName("MySparkApp") \
        .set("spark.sql.adaptive.enabled", "false") \
        .set("spark.sql.streaming.metricsEnabled", "true") \
        .set("spark.driver.memory", "2g") \
        .set("spark.driver.cores", "2") \
        .set("spark.driver.maxResultSize", "2g") \
        .set("spark.executor.instances", "1") \
        .set("spark.executor.cores", "4") \
        .set("spark.executor.memory", "3g") \
        .set("spark.dynamicAllocation.enabled", "true") \
        .set("spark.dynamicAllocation.minExecutors", "1") \
        .set("spark.dynamicAllocation.maxExecutors", "3") \
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .set("spark.sql.catalog.warehousedev", "org.apache.iceberg.spark.SparkCatalog") \
        .set("spark.sql.catalog.warehousedev.type", "rest") \
        .set("spark.sql.catalog.warehousedev.uri", "http://rest-catalog:8181") \
        .set("spark.sql.catalog.warehousedev.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .set("spark.sql.catalog.warehousedev.warehouse", "s3://warehousedev") \
        .set("spark.sql.catalog.warehousedev.s3.endpoint", "http://minio:9000") \
        .set("spark.sql.catalog.warehousedev.s3.path-style-access", "true") \
        .set("spark.sql.catalog.warehousedev.s3.region", "us-east-1") \
        .set("spark.sql.catalog.warehousedev.s3.access-key-id", "minioadmin") \
        .set("spark.sql.catalog.warehousedev.s3.secret-access-key", "minioadmin") \
        .set("spark.sql.defaultCatalog", "warehousedev") \
        .set("spark.sql.catalogImplementation", "in-memory") \
        .set("spark.executor.extraJavaOptions", "-Daws.region=us-east-1") \
        .set("spark.executorEnv.AWS_REGION", "us-east-1") \
        .set("spark.executorEnv.AWS_ACCESS_KEY_ID", "minioadmin") \
        .set("spark.executorEnv.AWS_SECRET_ACCESS_KEY", "minioadmin") \
        .set("spark.driver.extraJavaOptions", "-Daws.region=us-east-1") \
        .set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .set("spark.hadoop.fs.s3a.path.style.access", "true") \
        .set("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .set("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # .set("spark.sql.legacy.timeParserPolicy", "CORRECTED")  # 추가

    if not dev:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
            
    else:
        spark = SparkSession.builder.config(conf=dev_conf).getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def start_console_stream(df: DataFrame) -> StreamingQuery:
    return df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()

def get_kafka_stream_df(spark_session: SparkSession, _topic_names: Union[Iterable[str], str]) -> DataFrame:
    """
    - .option("maxOffsetsPerTrigger", "10000")
        : 배치당 처리할 Kafka 오프셋 최대 수. 처리량 제어.
    """
    # The type of `spark_session.readStream` is `DataStreamReader`
    # .option("maxOffsetsPerTrigger", "20000")  # 배치당 최대 오프셋, 조정 필요
    # 추가: spark.streaming.kafka.maxRatePerPartition (파티션당 초당 메시지 수 제한, 예: 1000) 설정으로 입력 속도 제어.
    # .option("groupId", "bronze2silver") \

    if isinstance(_topic_names, str):
        topic_names = [_topic_names]
    else:
        topic_names = _topic_names
        
    src_stream_df = spark_session.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS_INTERNAL) \
        .option("subscribe", ','.join(topic_names)) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "20000") \
        .load()
    return src_stream_df.select(col("key"), col("value"), col("topic"))


def get_deserialized_stream_df(kafka_stream_df: DataFrame, schema_str: str, key_column: str) -> DataFrame:
    if schema_str is not None:
        deserialized_key = col('key').cast("string").alias(key_column)
        deserialized_value = \
            from_avro(
                expr("substring(value, 6, length(value)-5)"), # Magic byte + schema id 제거
                schema_str,
                DESERIALIZE_OPTIONS
            ).alias("data")
        return kafka_stream_df.select(deserialized_key, deserialized_value).select(key_column, "data.*")
    
    # 추론된 스키마를 사용해서 스트리밍 데이터 처리
    return kafka_stream_df.select(
        col('key').cast('string').alias(key_column),
        from_json(col("value").cast("string"), kafka_stream_df.schema).alias("data")
    )