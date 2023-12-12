from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.snowflake.transfers.postgres_to_snowflake import PostgresToSnowflakeOperator
from datetime import datetime, timedelta


# Define default_args and schedule_interval
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'postgres_to_snowflake',
    default_args=default_args,
    schedule_interval='@daily',
)

extract_postgres_task = PostgresOperator(
    task_id='extract_postgres_data',
    sql='sql/extract_postgres_data.sql',
    postgres_conn_id='postgres123',
    dag=dag,
)


load_snowflake_task = PostgresToSnowflakeOperator(
    task_id='load_snowflake_data',
    sql='sql/load_snowflake_data.sql',
    postgres_conn_id='postgres123',
    snowflake_conn_id='subhanliaqat',
    schema='flights',
    table='flights',
    dag=dag,
)


# Define task dependencies
extract_postgres_task >> load_snowflake_task
