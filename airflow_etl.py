from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from sqlalchemy import create_engine
import requests
import pandas as pd

default_args = {
    "owner": "Subhan",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

@dag (
    dag_id = "flights_etl",
    start_date = datetime(2023, 12, 10),
    default_args = default_args,
    schedule = "@daily",
    catchup = False
)

def pipeline():

    @task
    def get_flights_data(start_date = datetime.now()):
    
        flights_data = pd.read_csv(csv_files['flights'])
        
        # getting the data into a dataframe and dropping unnecessary info, converting the time column type to datetime.
       flights_data = flights_data.drop(["YEAR", "WEATHER_DELAY", "WEATHER_DELAY", "AIRLINE_DELAY", "SECURITY_DELAY", "AIR_SYSTEM_DELAY"], axis=1)       
       return flights_data;
    
    # Checking if the DF is empty or contains Null values and raising an exception if either is true.
    @task
    def check_data(flights_data):

        if flights_data.empty:
            raise Exception("Dataframe is empty")
        elif flights_data.isna().sum().sum() > 0:
            raise Exception("Dataframe contains Null values")

    # Pandas dataframe to Snowflake  
    @task(trigger_rule = "all_success")
    def df_to_snowflake(flights_data):
        
        snowflake_hook = SnowflakeHook(snowflake_conn_id = "subhanliaqat")
        conn = snowflake_hook.get_uri()
        engine = create_engine(conn)
        flights_data.to_sql("flights", con = engine, index = False, if_exists = "append")

    # Creates the table to snowflake, using the connection that has been defined in Airflow Web UI
    task0 = SnowflakeOperator(
        task_id = "create_table",
        sql = "CREATE TABLE flights",
        snowflake_conn_id = "subhanliaqat"
    )

    task1 = get_flights_data()

    task2 = check_data(task1)

    task3 = SnowflakeOperator(
        task_id = "clear_table",
        trigger_rule = "all_success",
        sql = "DELETE FROM flights_data",
        snowflake_conn_id = "subhanliaqat"
    )

    task4 = df_to_snowflake(task1)



    task0 >> task1 >> task2 >> task3 >> task4

pipeline()
