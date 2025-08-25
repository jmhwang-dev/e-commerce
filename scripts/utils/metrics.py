from typing import List 
from jmxquery import JMXConnection, JMXQuery
import pandas as pd
import re

SPARK_EXECUTOR_PORT = 19085
SPARK_DRIVER_PORT = 19082
KAFKA_PRODUCER_PORT = 19999

queris_by_port = {
    KAFKA_PRODUCER_PORT: [JMXQuery("*.*:*")],
    SPARK_DRIVER_PORT: [JMXQuery("metrics:*")],
    SPARK_EXECUTOR_PORT: [JMXQuery("metrics:*"), JMXQuery("kafka.consumer:type=*,client-id=*")],
}

def get_query_results(port: int, queries: List[JMXQuery]) -> List[JMXQuery]:
    uri = f"service:jmx:rmi:///jndi/rmi://localhost:{port}/jmxrmi"
    jmx = JMXConnection(uri)
    return jmx.query(queries)

def get_metrics(query_results: List[JMXQuery]) -> List[dict]:
    metrics = []
    # 결과 출력 (MBean 이름과 값)
    for result in query_results:
        metric = {
            "mBeanName": result.mBeanName,
            "attribute": result.attribute,
            "value": result.value,
        }
        metrics.append(metric)
    return metrics

def save_metrics(metrics: List[dict], tsv_filename):
    df = pd.DataFrame(metrics)
    df.sort_values(by=["mBeanName", "attribute"], ascending=False, inplace=True)
    df.to_csv(tsv_filename, sep='\t', index=False)
    print(f'Saved: {tsv_filename}')

if __name__=="__main__":
    tsv_filename_prefix = 'metrics'

    for port, queries in queris_by_port.items():
        query_results = get_query_results(port, queries)

        if len(query_results) == 0:
            print(f"No JMX metrics for {port} found. Please check the JMX connection and queries.")
            continue

        metrics = get_metrics(query_results)

        if port == KAFKA_PRODUCER_PORT:
            tsv_filename = f"{tsv_filename_prefix}_kafka_producer.tsv"
        elif port == SPARK_DRIVER_PORT:
            tsv_filename = f"{tsv_filename_prefix}_spark_driver.tsv"
        elif port == SPARK_EXECUTOR_PORT:
            tsv_filename = f"{tsv_filename_prefix}_spark_executor.tsv"

        save_metrics(metrics, tsv_filename)

    print('Done')