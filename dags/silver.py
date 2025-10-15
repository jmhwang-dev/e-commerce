from airflow import DAG
from airflow.sdk import task

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

SRC_PATH = '/opt/airflow/src'
ARFTIFACT_DIR_PATH = '/opt/airflow/artifact'
ZIP_DST_PATH = f'{ARFTIFACT_DIR_PATH}/src.zip'

def get_spark_submit_operator(app_name):
    # TODO: add connections automatically.
    # 1. Airflow UI에서 Admin > Connections로 이동
    # 2. 'spark_default' 연결 생성 또는 편집. 아래 conn_id와 동일해야함.

    app_list = [
        'deduplicate', 
        'order_customer',
        'order_timeline',
        'order_transaction',
        'product_metadata',
    ]

    if app_name not in app_list:
        return

    app_name_path = f"silver/{app_name}.py"

    return SparkSubmitOperator(
        task_id=app_name,
        application=f'/opt/airflow/jobs/batch/{app_name_path}',  # The path to your Spark application file
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
    dag_id='silver',
    start_date=datetime(2016, 9, 4),
    schedule=None,  # Set to a schedule like '@daily' or None for manual runs
    catchup=True,
    tags=['spark', 'silver']
) as dag:
    
    py_files = zip_src()
    deduplicate = get_spark_submit_operator('deduplicate')
    order_customer = get_spark_submit_operator('order_customer')
    order_timeline = get_spark_submit_operator('order_timeline')
    order_transaction = get_spark_submit_operator('order_transaction')
    product_metadata = get_spark_submit_operator('product_metadata')

    py_files \
    >> deduplicate \
    >> order_customer \
    >> order_timeline \
    >> order_transaction \
    >> product_metadata 