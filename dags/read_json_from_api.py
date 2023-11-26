from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.http_sensor import HttpSensor
import json
import psycopg2
import os

TABLE_NAME = 'fema_disaster'

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'api_to_postgres',
    default_args=default_args,
    description='A DAG to read JSON from API and save to PostgreSQL',
    schedule_interval=timedelta(days=1),
)

def fetch_data_from_api(**kwargs):
    # Fetch JSON data from API
    response = kwargs['ti'].xcom_pull(task_ids='wait_for_api')
    api_data = response.json()

    return api_data

def read_and_save_to_postgres(**kwargs):
    api_data = kwargs['ti'].xcom_pull(task_ids='fetch_data_from_api')

    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host= PG_HOST,
        database= PG_DATABASE,
        user= PG_USER,
        password= PG_PASSWORD
    )

    # Insert data into PostgreSQL
    with conn.cursor() as cursor:
        for record in api_data:
            create_table = cursor.execute("create table" + TABLE_NAME + "{disaster_number varchar(20), year varchar(5), region varhar(20), region_number varchar(10), state_abbreviation varchar(2), county varchar(10), declaration_date timestamp, disaster_type varchar(20), incident_type varchar(20), title varchar(20), incident_begin_date timestamp, incident_end_date timestamp, disaster_close_out_date timestamp, location_1 varchar(50) ")
            if create_table:
                cursor.execute("INSERT INTO " + TABLE_NAME + " (disaster_number varchar(20), year varchar(5), region varhar(20), region_number varchar(10), state_abbreviation varchar(2), county varchar(10), declaration_date timestamp, disaster_type varchar(20), incident_type varchar(20), title varchar(20), incident_begin_date timestamp, incident_end_date timestamp, disaster_close_out_date timestamp, location_1 varchar(50)) VALUES (%s, %s, ...)", (record['disaster_number'], record['year'], ...))
            else:
                print("unable to create table")

    conn.commit()
    conn.close()

wait_for_api = HttpSensor(
    task_id='wait_for_api',
    http_conn_id='postgres_conn_id',  # Define your HTTP connection in Airflow UI
    endpoint='https://opendata.utah.gov/resource/52ez-jvug.json',  # API endpoint
    request_params={},  # Additional request parameters if needed
    poke_interval=60,  # Poll every 60 seconds
    timeout=600,  # Give up after 600 seconds
    mode='poke',
    dag=dag,
)

fetch_data_task = PythonOperator(
    task_id='fetch_data_from_api',
    python_callable=fetch_data_from_api,
    provide_context=True,
    dag=dag,
)

read_and_save_task = PythonOperator(
    task_id='read_and_save_to_postgres',
    python_callable=read_and_save_to_postgres,
    provide_context=True,
    dag=dag,
)

wait_for_api >> fetch_data_task >> read_and_save_task
