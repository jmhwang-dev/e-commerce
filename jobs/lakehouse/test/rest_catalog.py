from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# 테이블 생성: my_catalog 카탈로그에 test 스키마를 생성하고 products 테이블 생성
spark.sql("""
CREATE TABLE IF NOT EXISTS my_catalog.test.products (
  id INT,
  name STRING
) USING iceberg
""")

# 데이터 삽입
spark.sql("""
INSERT INTO my_catalog.test.products VALUES (1, 'apple'), (2, 'banana')
""")

# 데이터 조회
spark.sql("SELECT * FROM my_catalog.test.products").show()