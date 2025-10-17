from service.batch.gold import *
from service.utils.iceberg import initialize_namespace

if __name__ == "__main__":
    spark_session = get_spark_session("delivered_order_location", dev=False)
    initialize_namespace(spark_session, 'gold')

    job_instance = DeliveredOrderLocation()
    job_instance.generate()
    job_instance.update_table()
    job_instance.spark_session.stop()