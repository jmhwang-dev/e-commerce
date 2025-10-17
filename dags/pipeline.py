from airflow import DAG
from airflow.sdk import task

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

SRC_PATH = '/opt/airflow/src'
ARFTIFACT_DIR_PATH = '/opt/airflow/artifact'
ZIP_DST_PATH = f'{ARFTIFACT_DIR_PATH}/src.zip'

class Medallion:
    SILVER = 'silver'
    GOLD = 'gold'

def get_spark_submit_operator(app_name):
    # TODO: add connections automatically.
    # 1. Airflow UI에서 Admin > Connections로 이동
    # 2. 'spark_default' 연결 생성 또는 편집. 아래 conn_id와 동일해야함.

    app_dict = {
        'customer': Medallion.SILVER,
        'seller': Medallion.SILVER,
        'geolocation': Medallion.SILVER,
        'delivered_order': Medallion.SILVER,
        'order_customer': Medallion.SILVER,
        'order_timeline': Medallion.SILVER,
        'order_transaction': Medallion.SILVER,
        'product_metadata': Medallion.SILVER,
        'sales': Medallion.GOLD,
        'delivered_order_location': Medallion.GOLD
    }

    if app_name not in app_dict:
        return

    app_path = f"{app_dict[app_name]}/{app_name}.py"

    return SparkSubmitOperator(
        task_id=app_name,
        application=f'/opt/airflow/jobs/batch/{app_path}',  # The path to your Spark application file
        conn_id='spark_default',  # Connection ID for your Spark cluster
        py_files=ZIP_DST_PATH,
        deploy_mode='client',
        name='spark_job_run',
        properties_file='/opt/airflow/config/spark-iceberg.conf'
        # application_args=['--input', 'hdfs://...', '--output', 'hdfs://...'] # Optional arguments for your Spark job
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
    
    py_files = zip_src()
    
    customer = get_spark_submit_operator('customer')
    seller = get_spark_submit_operator('seller')
    geolocation = get_spark_submit_operator('geolocation')
    delivered_order = get_spark_submit_operator('delivered_order')
    order_customer = get_spark_submit_operator('order_customer')
    order_timeline = get_spark_submit_operator('order_timeline')
    order_transaction = get_spark_submit_operator('order_transaction')
    product_metadata = get_spark_submit_operator('product_metadata')

    sales = get_spark_submit_operator('sales')
    delivered_order_location = get_spark_submit_operator('delivered_order_location')

    py_files >> customer
    py_files >> seller
    py_files >> geolocation
    py_files >> delivered_order
    py_files >> order_customer
    py_files >> order_timeline
    py_files >> order_transaction
    py_files >> product_metadata 

    [delivered_order, product_metadata, order_transaction ] >> sales
    [delivered_order, geolocation, customer, seller, product_metadata, order_transaction, order_customer] >> delivered_order_location
