from typing import List

from service.pipeline.stream.silver import *
from service.batch.gold import *
from schema.silver import *

if __name__ == "__main__":
    spark_session = get_spark_session("Batch test", dev=True)
    initialize_namespace(spark_session, 'silver', is_drop=False)
    initialize_namespace(spark_session, 'gold', is_drop=True)

    job_list: List[StreamSilverJob] = [
        # SellerDeducplicator(),
        # DeliveredOrder(),
        # OrderTimeline(),
        # OrderCustomer(),
        # ProductMetadata(),
        # OrderTransaction(),

        SalesAggregator(),
        ProductPortfolioMatrix(),

        # DeliveredOrderLocation(),
        # OrderLeadDays(),
    ]

    i = 0
    end = 3

    while i < end:
        for job_instance in job_list:
            job_instance.generate()
            job_instance.update_table()
            df = job_instance.spark_session.read.table(job_instance.dst_table_identifier)
            df.show()
            # job_instance.update_watermark()
        i += 1

    for job_instance in job_list:
        job_instance.spark_session.stop()