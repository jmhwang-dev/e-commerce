from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

def get_max_count_cities_by_zip_code(geolocation):
    city_counts = (
        geolocation.groupBy("geolocation_zip_code_prefix", "geolocation_city")
        .count()
    )

    window_spec = Window.partitionBy("geolocation_zip_code_prefix").orderBy(F.desc("count"))
    max_count_cities = (
        city_counts.withColumn("rank", F.row_number().over(window_spec))
        .filter("rank = 1")
        .drop("count", "rank")
    )
    return max_count_cities

if __name__=="__main__":
    spark = SparkSession.builder.appName('geolocation').getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    geolocation = spark.read.table("warehouse_dev.silver.dedup.olist_geolocation_dataset")
    max_count_cities = get_max_count_cities_by_zip_code(geolocation)
    max_count_cities.show()