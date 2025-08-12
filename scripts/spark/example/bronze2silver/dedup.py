import re
from typing import List
from pyspark.sql import SparkSession

def get_paths(session: SparkSession, bucket: str, prefix: str) -> List[str]:
    """
    지정한 S3 버킷/prefix에서 .csv 파일 목록만 추출.
    JVM Hadoop API를 직접 사용하여 모든 파일 리스트를 가져온다.
    
    Args:
        session: SparkSession 객체
        bucket: S3 버킷명
        prefix: S3 내 폴더/경로(prefix), 예: 'bronze'
    Returns:
        List[str]: s3a 경로의 .csv 파일 리스트
    """
    # S3 경로 문자열 생성 (s3a 프로토콜 사용)
    path_str = f"s3a://{bucket}/{prefix}"
    
    # JVM 기반 Hadoop Path 객체 생성 (Spark에서 내부적으로 사용)
    jPath = session._jvm.org.apache.hadoop.fs.Path(path_str)
    
    # Hadoop FileSystem 객체 얻기 (권한/설정은 spark-defaults.conf를 따름)
    fs = jPath.getFileSystem(session._jsc.hadoopConfiguration())
    
    # 해당 디렉토리의 모든 파일/디렉터리 상태 리스트업
    status_list = fs.listStatus(jPath)
    
    # 파일 중 확장자가 .csv인 파일만 추출
    file_paths = [
        f.getPath().toString()
        for f in status_list
        if f.isFile() and f.getPath().getName().endswith(".csv")
    ]
    return file_paths

def dedup_and_save(spark: SparkSession, file_path: str, dst_namespace: str):
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    dedup_df = df.dropDuplicates()
    table_name = re.sub(r'\W+', '_', file_path.split("/")[-1].replace(".csv", ""))

    full_table_name = f"{dst_namespace}.{table_name}"
    print(f"{full_table_name}, before: {df.count()}, after: {dedup_df.count()}")

    if not spark.catalog.tableExists(full_table_name):
        dedup_df.writeTo(full_table_name).create()
    else:
        dedup_df.writeTo(full_table_name).overwritePartitions()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("deduplicate_bronze").getOrCreate()
    try:
        spark.sparkContext.setLogLevel("WARN")
        BUCKET = "warehouse-dev"
        BRONZE_PREFIX = "bronze"
        DST_QUALIFIED_NAMESPACE = "warehouse_dev.silver.dedup"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {DST_QUALIFIED_NAMESPACE}")
        file_paths = get_paths(spark, BUCKET, BRONZE_PREFIX)
        for file_path in file_paths:
            try:
                dedup_and_save(spark, file_path, DST_QUALIFIED_NAMESPACE)
            except Exception as e:
                print(f"[ERROR] {file_path} 처리 실패: {e}")
    finally:
        spark.stop()
