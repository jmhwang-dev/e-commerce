import os

from airflow import DAG
from airflow.sdk import task

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

from service.utils.schema.avsc import *

SRC_PATH = '/opt/airflow/src'
ARFTIFACT_DIR_PATH = '/opt/airflow/artifact'
ZIP_DST_PATH = f'{ARFTIFACT_DIR_PATH}/src.zip'
SPARK_CONN_ID = os.getenv("SPARK_CONN_ID", "spark_defaultz")

def get_spark_submit_operator(app_name) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=app_name,
        application=f'/opt/airflow/jobs/run_dag.py',  # The path to your Spark application file
        conn_id=SPARK_CONN_ID,  # Connection ID for your Spark cluster
        py_files=ZIP_DST_PATH,
        deploy_mode='client',
        name='spark_job_run',
        properties_file='/opt/airflow/config/spark-iceberg.conf',
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
    schedule=None,  # Set to a schedule like '@daily' or None for manual runs
    catchup=True,
    tags=['spark', 'pipeline']
) as dag:
    
    PY_FILES = zip_src()

    GEO_COORD = get_spark_submit_operator(SilverAvroSchema.GEO_COORD)
    OLIST_USER = get_spark_submit_operator(SilverAvroSchema.OLIST_USER)
    REVIEW_METADATA = get_spark_submit_operator(SilverAvroSchema.REVIEW_METADATA)
    PRODUCT_METADATA = get_spark_submit_operator(SilverAvroSchema.PRODUCT_METADATA)
    CUSTOMER_ORDER = get_spark_submit_operator(SilverAvroSchema.CUSTOMER_ORDER)
    ORDER_EVENT = get_spark_submit_operator(SilverAvroSchema.ORDER_EVENT)

    DIM_USER_LOCATION = get_spark_submit_operator(GoldAvroSchema.DIM_USER_LOCATION)
    ORDER_DETAIL = get_spark_submit_operator(GoldAvroSchema.ORDER_DETAIL)
    FACT_ORDER_TIMELINE = get_spark_submit_operator(GoldAvroSchema.FACT_ORDER_TIMELINE)
    FACT_ORDER_LEAD_DAYS = get_spark_submit_operator(GoldAvroSchema.FACT_ORDER_LEAD_DAYS)
    FACT_MONTHLY_SALES_BY_PRODUCT = get_spark_submit_operator(GoldAvroSchema.FACT_MONTHLY_SALES_BY_PRODUCT)
    FACT_REVIEW_STATS = get_spark_submit_operator(GoldAvroSchema.FACT_REVIEW_STATS)
    MONTHLY_CATEGORY_PORTFOLIO_MATRIX = get_spark_submit_operator(GoldAvroSchema.MONTHLY_CATEGORY_PORTFOLIO_MATRIX)
    
    # batch for silver
    PY_FILES >> [
        GEO_COORD,
        OLIST_USER,
        REVIEW_METADATA,
        PRODUCT_METADATA,
        CUSTOMER_ORDER,
        ORDER_EVENT,
    ]

    # batch for gold
    ORDER_EVENT >> FACT_ORDER_TIMELINE
    [PRODUCT_METADATA, CUSTOMER_ORDER] >> ORDER_DETAIL
    [GEO_COORD, OLIST_USER]>> DIM_USER_LOCATION
    [REVIEW_METADATA, ORDER_DETAIL]>> FACT_REVIEW_STATS
    FACT_ORDER_TIMELINE >> FACT_ORDER_LEAD_DAYS
    [ORDER_DETAIL, FACT_ORDER_TIMELINE] >> FACT_MONTHLY_SALES_BY_PRODUCT
    FACT_MONTHLY_SALES_BY_PRODUCT >> MONTHLY_CATEGORY_PORTFOLIO_MATRIX