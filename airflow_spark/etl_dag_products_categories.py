# Пример ДАГа для организации ETL процесса для таблицы products_categories

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from datetime import datetime

TABLE_NAME = 'products_categories'
S3_URL = f's3a://s3.amazonaws.com/datasets/{TABLE_NAME}.parquet'

def load_to_clickhouse(**kwargs):
    query = f"""
        INSERT INTO {TABLE_NAME}
            SELECT * FROM s3('{S3_URL}', 'Parquet')
    """
    clickhouse_hook = ClickHouseHook(clickhouse_conn_id='conn_id')
    clickhouse_hook.execute(query)

default_args = {
    'max_retries': 3,
}

with DAG(
    f'etl_{TABLE_NAME}',
    default_args=default_args,
    schedule_interval='выбор интервала зависит от TABLE_NAME',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    spark_job_task = SparkSubmitOperator(
        task_id='spark_job',
        application='spark_job_products_categories.py',
        name='spark_et',
        conn_id='conn_id',
        application_args=[
            '--table_name', TABLE_NAME,
            '--s3_url', S3_URL,
        ],
    )

    load_to_clickhouse_task = PythonOperator(
        task_id='load_to_dest',
        python_callable=load_to_clickhouse,
    )

    spark_job_task >> load_to_clickhouse_task
