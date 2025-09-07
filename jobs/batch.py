from typing import List
from service.utils.spark import get_spark_session
from service.batch.bronze2silver import *
from service.utils.iceberg import append_or_create_table


if __name__ == "__main__":
    # TODO: 워크플로우 관리도구 추가, 증분처리 로그 추가
    spark = get_spark_session("Silver Layer Pipeline")

    silver_job_list: List[BronzeToSilverJob] = [
        CustomerBronzeToSilverJob,
        EstimatedDeliveryDateBronzeToSilverJob,
        GeolocationBronzeToSilverJob,
        OrderItemBronzeToSilverJob,
        ProductBronzeToSilverJob,
        SellerBronzeToSilverJob
    ]
    
    i = 0
    end = 2
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {BronzeToSilverJob.dst_namesapce}")
    append_or_create_table(spark, spark.createDataFrame([], WATERMARK_SCHEMA), BronzeToSilverJob.watermark_table)

    while i < end:
        try:
            for job_class in silver_job_list:
                job_instance: BronzeToSilverJob = job_class(spark)
                job_instance.set_incremental_df()
                destination_dfs = job_instance.transform()
                for table_identifier, df in destination_dfs.items():
                    append_or_create_table(spark, df, table_identifier)
                    print(f"Writing {df.count()} clean records to {table_identifier}...")
                update_watermark(spark, job_instance.watermark_table, job_instance.job_name, job_instance.end_snapshot_id)
                print(f"============== [{job_instance.job_name}] Job Finished ==============")
            i += 1
        finally:
            pass
    spark.stop()