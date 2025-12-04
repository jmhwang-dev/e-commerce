import os
from datetime import timedelta

from airflow import DAG
from airflow.sdk import task

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

from service.utils.schema.avsc import *

SRC_PATH = '/opt/airflow/src'
ARFTIFACT_DIR_PATH = '/opt/airflow/artifact'
ZIP_DST_PATH = f'{ARFTIFACT_DIR_PATH}/src.zip'
SPARK_CONN_ID = os.getenv("SPARK_CONN_ID", "spark_default")

def init_catalog(is_drop: bool) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id='init_catalog',
        application=f'/opt/airflow/jobs/batch/init.py',  # The path to your Spark application file
        conn_id=SPARK_CONN_ID,  # Connection ID for your Spark cluster
        py_files=ZIP_DST_PATH,
        deploy_mode='client',
        properties_file='/opt/spark/config/spark-defaults.conf',
        conf={ "spark.cores.max": "2" },
        application_args=['--is_drop'] if is_drop else []
    )

def get_spark_submit_operator(app_name) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=app_name,
        application=f'/opt/airflow/jobs/batch/task.py',  # The path to your Spark application file
        conn_id=SPARK_CONN_ID,  # Connection ID for your Spark cluster
        py_files=ZIP_DST_PATH,
        deploy_mode='client',
        properties_file='/opt/spark/config/spark-defaults.conf',
        conf={ "spark.cores.max": "2" },
        application_args=['--app_name', app_name] # Optional arguments for your Spark job
    )

@task.bash
def zip_src():
    # TODO: src.zip을 shared 폴더로 바인드 마운트하기
    return \
    f"""
    mkdir -p {ARFTIFACT_DIR_PATH} &&
    rm -f {ZIP_DST_PATH} && \
    cd {SRC_PATH} && \
    zip -r {ZIP_DST_PATH} service config schema > /dev/null
    """

with DAG(
    dag_id='pipeline',
    start_date=datetime(2016, 9, 4),
    schedule=timedelta(seconds=300),  # Set to a schedule like '@daily' or None for manual runs
    catchup=False,
    tags=['spark', 'pipeline']
) as dag:
    
    PY_FILES = zip_src()
    INIT_CATALOG = init_catalog(False)

    GEO_COORD = get_spark_submit_operator(SilverAvroSchema.GEO_COORD)
    OLIST_USER = get_spark_submit_operator(SilverAvroSchema.OLIST_USER)
    REVIEW_METADATA = get_spark_submit_operator(SilverAvroSchema.REVIEW_METADATA)
    PRODUCT_METADATA = get_spark_submit_operator(SilverAvroSchema.PRODUCT_METADATA)
    CUSTOMER_ORDER = get_spark_submit_operator(SilverAvroSchema.CUSTOMER_ORDER)
    ORDER_EVENT = get_spark_submit_operator(SilverAvroSchema.ORDER_EVENT)

    DIM_USER_LOCATION = get_spark_submit_operator(GoldAvroSchema.DIM_USER_LOCATION)
    FACT_ORDER_DETAIL = get_spark_submit_operator(GoldAvroSchema.FACT_ORDER_DETAIL)
    FACT_ORDER_LEAD_DAYS = get_spark_submit_operator(GoldAvroSchema.FACT_ORDER_LEAD_DAYS)
    FACT_MONTHLY_SALES_BY_PRODUCT = get_spark_submit_operator(GoldAvroSchema.FACT_MONTHLY_SALES_BY_PRODUCT)
    FACT_REVIEW_ANSWER_LEAD_DAYS = get_spark_submit_operator(GoldAvroSchema.FACT_REVIEW_ANSWER_LEAD_DAYS)
    
    # if resource is enough...
    # batch for silver
    # PY_FILES >> INIT_CATALOG >> [
    #     GEO_COORD,
    #     OLIST_USER,
    #     REVIEW_METADATA,
    #     PRODUCT_METADATA,
    #     CUSTOMER_ORDER,
    #     ORDER_EVENT,
    # ]
    # # batch for gold
    # ORDER_EVENT >> FACT_ORDER_LEAD_DAYS
    # [PRODUCT_METADATA, CUSTOMER_ORDER] >> FACT_ORDER_DETAIL
    # [GEO_COORD, OLIST_USER]>> DIM_USER_LOCATION
    # REVIEW_METADATA >> FACT_REVIEW_ANSWER_LEAD_DAYS
    # [FACT_ORDER_DETAIL, FACT_ORDER_LEAD_DAYS] >> FACT_MONTHLY_SALES_BY_PRODUCT


    # if resource is not enough...
    PY_FILES >> INIT_CATALOG >> \
        PRODUCT_METADATA >> CUSTOMER_ORDER >> FACT_ORDER_DETAIL >> \
        ORDER_EVENT >> FACT_ORDER_LEAD_DAYS >> FACT_MONTHLY_SALES_BY_PRODUCT >> \
        REVIEW_METADATA >> FACT_REVIEW_ANSWER_LEAD_DAYS >> \
        GEO_COORD >> OLIST_USER >> DIM_USER_LOCATION