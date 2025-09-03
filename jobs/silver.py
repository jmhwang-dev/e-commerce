from service.utils.spark import get_spark_session
from service.pipeline.silver import *

if __name__ == "__main__":
    # TODO: 워크플로우 관리도구 추가, 증분처리 로그 추가
    spark = get_spark_session("Silver Layer Pipeline")
    
    i = 0
    end = 5
    while i < end:
        try:
            # One-time setup for the pipeline
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SilverJob.clean_namespace}")
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SilverJob.error_namespace}")
            append_or_create_table(spark, spark.createDataFrame([], WATERMARK_SCHEMA), SilverJob.watermark_table)

            # Instantiate and run jobs
            payment_job = PaymentSilverJob(spark)
            payment_job.run()
            
            # You can add more jobs here
            # order_job = OrderSilverJob(spark)
            # order_job.run()
            i += 1

        finally:
            pass

    print("Pipeline finished. Stopping SparkSession.")
    df = spark.read.table('warehousedev.bronze.stream_payment')
    df.sort('timestamp').show(n= 100, truncate=False)
    print(df.count())
    df_clean = spark.read.table('warehousedev.silver.payment')
    df_error = spark.read.table('warehousedev.silver.error.payment')
    print(f"{df_clean.count()} + {df_error.count()}")
    spark.stop()