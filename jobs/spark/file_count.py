from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Bucket List Test") \
    .getOrCreate()

try:
    # Hadoop FileSystem API 사용
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jvm.java.net.URI("s3a://bronze/"), 
        hadoop_conf
    )
    
    # 파일 목록 조회
    path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path("s3a://bronze/")
    files = fs.listStatus(path)
    
    print("=== Bronze 버킷 파일 목록 ===")
    for file in files:
        print(f"- {file.getPath().getName()} ({file.getLen()} bytes)")
        
except Exception as e:
    print(f"연결 오류: {e}")
finally:
    spark.stop()
