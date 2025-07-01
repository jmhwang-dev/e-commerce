# jobs/quick_test.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Quick Test") \
    .getOrCreate()

# 작은 데이터로 테스트
data = [("test", 1)]
df = spark.createDataFrame(data, ["name", "value"])

# bronze 버킷에 저장
df.write.mode("overwrite").csv("s3a://bronze/quick")
print("✅ 연결 성공!")
spark.stop()
