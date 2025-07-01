# jobs/simple_test.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Simple S3A Test") \
    .getOrCreate()

try:
    df = spark.read.csv("s3a://bronze/olist_customers_dataset.csv")
    df.show()
    print("✅ 읽기 성공!")
except Exception as e:
    print(f"❌ 오류: {e}")
finally:
    spark.stop()
