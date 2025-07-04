from pyspark.sql import SparkSession
import os

# 환경변수 설정 (AWS SDK용)
os.environ['AWS_REGION'] = 'us-east-1'
os.environ['AWS_ACCESS_KEY_ID'] = 'minioadmin'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'minioadmin'

# Spark 세션 생성 시 Iceberg 설정 추가
spark = SparkSession.builder \
    .appName("IcebergTest") \
    .getOrCreate()


print("=== 1단계: 네임스페이스 생성 ===")
spark.sql("CREATE NAMESPACE IF NOT EXISTS my.test")

print("=== 2단계: 네임스페이스 확인 ===")
spark.sql("SHOW NAMESPACES IN my").show()

print("=== 3단계: 새 테이블 생성 ===")
spark.sql("DROP TABLE IF EXISTS my.test.products2")
spark.sql("""
CREATE TABLE my.test.products2 (
    id INT,
    name STRING
)
USING iceberg
""")
print("새 테이블 생성 완료")

print("=== 4단계: 테이블 목록 확인 ===")
spark.sql("SHOW TABLES IN my.test").show()

print("=== 5단계: 데이터 삽입 ===")
spark.sql("""
INSERT INTO my.test.products2 VALUES (1, 'apple'), (2, 'banana')
""")

print("=== 6단계: 데이터 조회 ===")
spark.sql("SELECT * FROM my.test.products2").show()

print("=== 7단계: 테이블 스냅샷 확인 ===")
spark.sql("SELECT * FROM my.test.products2.snapshots").show()

# Spark 세션 종료
spark.stop()