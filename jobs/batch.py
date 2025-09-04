from typing import List
from service.utils.spark import get_spark_session
from service.pipeline.batch import *
from service.loader.iceberg import load_to_iceberg

if __name__ == "__main__":
    # TODO: 워크플로우 관리도구 추가, 증분처리 로그 추가
    spark = get_spark_session("Silver Layer Pipeline")

    silver_job_list: List[SilverJob] = [
        CustomerSilverJob,
        EstimatedDeliveryDateSilverJob,
        GeolocationSilverJob,
        OrderItemSilverJob,
        ProductSilverJob,
        SellerSilverJob
    ]
    
    i = 0
    end = 2
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SilverJob.clean_namespace}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SilverJob.quarantine_namespace}")
    append_or_create_table(spark, spark.createDataFrame([], WATERMARK_SCHEMA), SilverJob.watermark_table)

    while i < end:
        try:
            for job_class in silver_job_list:
                job_instance: SilverJob = job_class(spark)
                job_instance.set_incremental_df()
                destination_dfs = job_instance.transform()
                load_to_iceberg(spark, destination_dfs)
                update_watermark(spark, job_instance.watermark_table, job_instance.job_name, job_instance.end_snapshot_id)
                print(f"============== [{job_instance.job_name}] Job Finished ==============")
            i += 1
        finally:
            pass
    spark.stop()