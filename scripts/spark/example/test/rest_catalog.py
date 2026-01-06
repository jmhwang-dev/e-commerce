from pyspark.sql import SparkSession
import os

# Spark 세션 생성 시 Iceberg 설정 추가
spark = SparkSession.builder \
    .appName("IcebergTest") \
    .getOrCreate()


print("=== 1단계: 네임스페이스 생성 ===")
spark.sql("CREATE NAMESPACE IF NOT EXISTS dev.test")

print("=== 2단계: 네임스페이스 확인 ===")
spark.sql("SHOW NAMESPACES IN dev").show()

print("=== 3단계: 새 테이블 생성 ===")
spark.sql("DROP TABLE IF EXISTS dev.test.products")
spark.sql("""
CREATE TABLE dev.test.products (
    id INT,
    name STRING
)
USING iceberg
""")
print("새 테이블 생성 완료")

print("=== 4단계: 테이블 목록 확인 ===")
spark.sql("SHOW TABLES IN dev.test").show()

print("=== 5단계: 데이터 삽입 ===")
spark.sql("""
INSERT INTO dev.test.products VALUES (1, 'apple'), (2, 'banana')
""")

print("=== 6단계: 데이터 조회 ===")
spark.sql("SELECT * FROM dev.test.products").show()

print("=== 7단계: 테이블 스냅샷 확인 ===")
spark.sql("SELECT * FROM dev.test.products.snapshots").show()

# Spark 세션 종료
spark.stop()