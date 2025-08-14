from pyspark.sql import SparkSession

def get_spark_session(app_name: str) -> SparkSession:
    """
    confs

    - spark.conf.set("spark.sql.shuffle.partitions", "50")
        : 셔플 작업의 파티션 수. 스트리밍 병렬 처리 성능에 영향.
    
    - spark.conf.set("spark.sql.streaming.commitIntervalMs", "5000")
        : Kafka 오프셋 커밋 주기(ms). 빠른 커밋 vs 안정성 조절.

    - spark.conf.set("spark.hadoop.fs.s3a.multipart.size", "268435456") // 256MB
        : minIO에 업로드 시 멀티파트 크기. 큰 데이터 전송 성능 최적화.

    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark