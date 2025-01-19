# Пример ДАГа для организации ETL процесса для таблиц sales, ads

# ДАГ для таблицы ads будет аналогичным, нужно
# лишь поменять значение переменной TABLE_NAME и выставить корректный 
# интервал для запуска ДАГа


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from datetime import datetime

TABLE_NAME = 'sales'
S3_URL = f's3a://s3.amazonaws.com/datasets/{TABLE_NAME}.parquet'

def get_last_update_time(**kwargs):
    clickhouse_hook = ClickHouseHook(clickhouse_conn_id='conn_id')
    query = f"SELECT max(date) FROM {TABLE_NAME}"
    result = clickhouse_hook.execute(query)
    last_update_time = result[0][0]
    if not last_update_time:
        last_update_time = '2023-12-31'
    kwargs['ti'].xcom_push(key='last_update_time', value=last_update_time)


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

    get_last_update_time_task = PythonOperator(
        task_id='get_last_update_time',
        python_callable=get_last_update_time,
    )

    spark_job_task = SparkSubmitOperator(
        task_id='spark_job',
        application='spark_job.py',
        name='spark_et',
        conn_id='conn_id',
        application_args=[
            '--table_name', TABLE_NAME,
            '--s3_url', S3_URL,
            '--last_update_time', "{{ task_instance.xcom_pull(task_ids='get_last_update_time', key='last_update_time') }}"
        ],
    )

    load_to_clickhouse_task = PythonOperator(
        task_id='load_to_dest',
        python_callable=load_to_clickhouse,
    )

    get_last_update_time_task >> spark_job_task >> load_to_clickhouse_task
