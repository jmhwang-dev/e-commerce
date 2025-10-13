
from typing import List

from service.batch.silver import *
from schema.silver import *

from service.utils.spark import get_spark_session
from service.producer.bronze import BronzeTopic
from service.utils.iceberg import reset_namespace

SPARK_SESSION = get_spark_session("Silver")
BRONZE_NAMESPACE = 'bronze'
SILVER_NAMESPACE = 'silver'

if __name__ == "__main__":
    reset_namespace()
    # TODO: 워크플로우 관리도구 추가, 증분처리 로그 추가
    spark = get_spark_session("Silver Batch Job", dev=True)
    silver_batch_job_list: List[SilverBatchJob] = [
        OrderProduct,
    ]

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SilverBatchJob.dst_namesapce}")


    df = spark.read.table('warehousedev.bronze.product').drop_duplicates() # always read all
    df.show()
    spark.stop()


    # while i < end:
    #     try:
    #         for job_class in silver_batch_job_list:
    #             job_instance: SilverBatchJob = job_class(spark)
    #             print('들어오긴하나?')
    #             print('---'* 50)
    #             print('---'* 50)
    #             print('---'* 50)
    #             print('---'* 50)
    #             print('---'* 50)
    #             job_instance.generate()
    #             # job_instance.update_table()
    #             # job_instance.update_watermark()
    #         i += 1
    #     finally:
    #         pass
    # spark.stop()