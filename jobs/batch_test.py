
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
    reset_namespace(SILVER_NAMESPACE, is_drop=False)
    spark = get_spark_session("Silver Batch Job", dev=True)
    
    job_instance: SilverBatchJob = OrderTimeline(spark)
    job_instance.generate()
    # job_instance.update_table()
    # job_instance.update_watermark()

    SPARK_SESSION.stop()