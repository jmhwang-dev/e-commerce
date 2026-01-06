from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType

def get_max_count_cities_by_zip_code(geolocation):
    city_counts = (
        geolocation
        .withColumn(
            "geolocation_city",
            F.regexp_replace(F.col("geolocation_city"), "[ãáàâ]", "a")  # 악센트 정규화
        )
        .groupBy("geolocation_zip_code_prefix", "geolocation_city")
        .count()
    )
    window_spec = Window.partitionBy("geolocation_zip_code_prefix").orderBy(F.desc("count"))
    max_count_cities = (
        city_counts
        .withColumn("rank", F.row_number().over(window_spec))
        .filter("rank = 1")
        .select("geolocation_zip_code_prefix", "geolocation_city")  # 명시적 컬럼 선택
    )
    return max_count_cities

if __name__ == "__main__":
    spark = SparkSession.builder.appName("geolocation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 테이블 로드
    try:
        SRC_TABLE_NAME = "warehousedev.silver.dedup.olist_geolocation_dataset"
        geolocation = spark.read.table(SRC_TABLE_NAME)
    except Exception as e:
        print(f"Error loading table: {e}")
        spark.stop()
        exit(1)

    max_count_cities = get_max_count_cities_by_zip_code(geolocation)

    # 원본 geolocation에서 city를 대체
    new_geolocation = (
        geolocation.alias("geo")
        .drop("geolocation_city")
        .join(max_count_cities.alias("proc"), on="geolocation_zip_code_prefix", how="left")
        .select(
            F.col("geo.geolocation_zip_code_prefix"),
            F.col("geo.geolocation_lat"),
            F.col("geo.geolocation_lng"),
            F.coalesce(F.col("proc.geolocation_city"), F.lit("unknown")).alias("geolocation_city"),
            F.col("geo.geolocation_state")
        )
    ).cache()  # 캐싱

    # 중복 제거 및 위경도 평균 계산
    final_geolocation = (
        new_geolocation
        .groupBy(
            "geolocation_zip_code_prefix",
            "geolocation_city",
            "geolocation_state"
        )
        .agg(
            F.mean("geolocation_lat").alias("geolocation_lat"),
            F.mean("geolocation_lng").alias("geolocation_lng")
        )
    )

    # 데이터 무결성 점검
    unique_zip_original = geolocation.select("geolocation_zip_code_prefix").distinct().count()
    unique_zip_final = final_geolocation.select("geolocation_zip_code_prefix").distinct().count()
    print("Zip code 보존 여부:", unique_zip_original == unique_zip_final)

    original_cities = geolocation.select("geolocation_city").distinct().count()
    final_cities = final_geolocation.select("geolocation_city").distinct().count()
    print(f"Original unique cities: {original_cities}, Final unique cities: {final_cities}")

    geolocation_count = geolocation.count()
    final_geolocation_count = final_geolocation.count()
    print("위경도 평균 계산 전 geolocation 레코드 수:", geolocation_count)
    print("위경도 평균 계산 후 geolocation 레코드 수:", final_geolocation_count)

    # Null 값 점검
    new_geolocation.select(
        F.sum(F.col("geolocation_city").isNull().cast("int")).alias("null_count_geolocation_city")
    ).show()


    DST_QUALIFIED_NAMESPACE = "warehousedev.silver.geolocation"
    DST_TABLE_NAME = "unified_geolocation"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {DST_QUALIFIED_NAMESPACE}")
    full_table_name = f"{DST_QUALIFIED_NAMESPACE}.{DST_TABLE_NAME}"

    writer = (
        new_geolocation.writeTo(full_table_name)
        .tableProperty(
            "comment",
            "Unified geolocation data by zip code prefix, using the most frequent city name and averaged latitude/longitude. "
            "Derived from `warehousedev.silver.dedup.olist_geolocation_dataset` with duplicates removed."
        )
    )

    if not spark.catalog.tableExists(full_table_name):
        writer.create()
    else:
        writer.overwritePartitions()

    print(f"[INFO] {full_table_name} 테이블 저장 완료")
    new_geolocation.show(n=5)
    spark.stop()