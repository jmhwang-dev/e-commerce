from service.batch.silver import *
from service.utils.iceberg import initialize_namespace

if __name__ == "__main__":
    spark_session = get_spark_session("deduplicate customer", dev=False)
    initialize_namespace(spark_session, 'silver')

    job_instance = CustomerDeducplicator()
    job_instance.generate()
    job_instance.update_table()
    job_instance.spark_session.stop()