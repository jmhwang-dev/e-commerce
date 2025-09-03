from service.utils.spark import get_spark_session
from service.pipeline.silver import *

if __name__ == "__main__":
    # TODO: 워크플로우 관리도구 추가, 증분처리 로그 추가
    spark = get_spark_session("Silver Layer Pipeline")
    
    i = 0
    end = 2
    while i < end:
        try:
            # One-time setup for the pipeline
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SilverJob.clean_namespace}")
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SilverJob.error_namespace}")
            append_or_create_table(spark, spark.createDataFrame([], WATERMARK_SCHEMA), SilverJob.watermark_table)

            # Instantiate and run jobs
            payment_job = PaymentSilverJob(spark)
            payment_job.run()
            
            review_job = ReviewSilverJob(spark)
            review_job.run()
            # You can add more jobs here
            # order_job = OrderSilverJob(spark)
            # order_job.run()
            i += 1

        finally:
            pass

    print("Pipeline finished. Stopping SparkSession.")
    df = spark.read.table('warehousedev.bronze.stream_payment')
    print(df.count())
    df_clean = spark.read.table('warehousedev.silver.payemnt')
    df_metdadata = spark.read.table('warehousedev.silver.error.payment')
    print(f"{df_clean.count()}")
    print(f"{df_metdadata.count()}")

    df.sort('timestamp').show(n= 100, truncate=False)
    df_clean.sort('timestamp').show(n= 100, truncate=False)
    df_metdadata.sort('timestamp').show(n= 100, truncate=False)

    print("Pipeline finished. Stopping SparkSession.")
    df = spark.read.table('warehousedev.bronze.stream_review')
    print(df.count())
    df_clean = spark.read.table('warehousedev.silver.review_clean_comment')
    df_metdadata = spark.read.table('warehousedev.silver.review_metadata')
    print(f"{df_clean.count()}")
    print(f"{df_metdadata.count()}")

    df.sort('review_id').show(n= 100, truncate=False)
    df_clean.sort('review_id').show(n= 100, truncate=False)
    df_metdadata.sort('review_id').show(n= 100, truncate=False)

    # df_error = spark.read.table('warehousedev.silver.error.payment')
    # print(f"{df_clean.count()} + {df_error.count()}")
    spark.stop()