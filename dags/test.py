from airflow import DAG
from airflow.sdk import task

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime

# spark-submit --master spark://spark-master:7077 --properties-file /opt/airflow/config/spark-iceberg.conf --py-files /opt/airflow/jobs/batch/src.zip /opt/airflow/jobs/batch/silver.py
# TODO: src.zip을 shared 폴더로 바인드 마운트하기

SRC_PATH = '/opt/airflow/src'
ARFTIFACT_DIR_PATH = '/opt/airflow/artifact'
ZIP_DST_PATH = f'{ARFTIFACT_DIR_PATH}/src.zip'
with DAG(
    dag_id='spark_job_example',
    start_date=datetime(2023, 1, 1),
    schedule=None,  # Set to a schedule like '@daily' or None for manual runs
    catchup=False,
    tags=['spark', 'example']
) as dag:
    
    @task.bash
    def zip_src():
        return \
        f"""
        mkdir -p {ARFTIFACT_DIR_PATH} &&
        rm -f {ZIP_DST_PATH} && \
        cd {SRC_PATH} && \
        zip -r {ZIP_DST_PATH} service config schema > /dev/null
        """

    py_files = zip_src()
    

    submit_spark_job = SparkSubmitOperator(
        task_id='submit_spark_job_task',
        application='/opt/airflow/jobs/batch_test.py',  # The path to your Spark application file
        conn_id='spark_default',  # Connection ID for your Spark cluster
        py_files=ZIP_DST_PATH,
        deploy_mode='client',
        name='spark_job_run',
        properties_file='/opt/airflow/config/spark-iceberg.conf'
        # application_args=['--input', 'hdfs://...', '--output', 'hdfs://...'] # Optional arguments for your Spark job
    )

    py_files >> submit_spark_job