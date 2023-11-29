import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.db_conn import db_connection
from utils.postgres_to_gcs import to_gcs
from utils.gcs_to_bigquery import gcs_to_bigquery


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')
SOCRATA_APP_TOKEN = os.getenv('SOCRATA_APP_TOKEN')

BUCKET_NAME =  os.getenv('GCP_GCS_BUCKET')
FILE_PATH = os.getenv('GCP_FILE')

# URL_PREFIX = 'https://car-api2.p.rapidapi.com/api/vin/1GTG6CEN0L1139305'
# URL_PREFIX = 'https://aerodatabox.p.rapidapi.com/airports/%7BcodeType%7D/LHR'

APP_TOKEN = SOCRATA_APP_TOKEN

URL_PREFIX = "https://opendata.utah.gov/resource/52ez-jvug.json?"

API_KEY = os.getenv('API_KEY')
API_HOST = os.getenv('API_HOST')

URL_BUILD = URL_PREFIX  #+ '$$app_token='+  APP_TOKEN # ',' + 'headers={X-App-Token='+ APP_TOKEN + '}'
# URL_BUILD = URL_PREFIX + ',' + 'headers={' + '"X-RapidAPI-Key:"'+ API_KEY + ',' + '"X-RapidAPI-Host:"' + API_HOST +'}'

URL_TEMPLATE = URL_BUILD
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output.json'
# OUTPUT_FILE_TEMPLATE = '/Users/semiubiliaminu/my_folder/microclick/altschool_project/final_capstone_project/car_list.json'
#/opt/airflow/output_2021-01.csv.gz

# TABLE_NAME_TEMPLATE = 'us_car_list'

TABLE_NAME_TEMPLATE = 'disasters'




DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 1),
    "email": [os.getenv("ALERT_EMAIL", "")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="LoadDataFromWebToDB",
    description="Job to move data from socrata website to local Postgresql DB",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 2 * *", # At 06:00 on day 2 of every month
    max_active_runs=1,
    catchup=True,
    tags=["Website_to_local_postgresql_DB"],
) as dag:

    start = EmptyOperator(task_id="start")

    download_file = BashOperator(
         task_id = "download_file",
         bash_command = f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )

    ingestion_data = PythonOperator(
        task_id ="ingestion_data",
        python_callable=db_connection,
        op_kwargs=dict(
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            json_file=OUTPUT_FILE_TEMPLATE
        ),
        dag = dag
    )

    delete_file = BashOperator(
         task_id = "delete_file",
         bash_command = f'rm {OUTPUT_FILE_TEMPLATE}'
    )

    load_to_gcs = PythonOperator(
        task_id = "loadtoGCS",
        python_callable = to_gcs,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE
        )
    )

    # load_to_gcs = BashOperator(
    #     task_id = "loadtoGCS",
    #     bash_command = f'pg_dump -h {PG_HOST} -U {PG_USER} -d {PG_DATABASE} -t {TABLE_NAME_TEMPLATE} --format=table --file={FILE_PATH}'
    #     # pg_dump -h your_postgres_host -U your_user -d your_database -t your_table --format=csv --file=your_table_data.csv

    # )

    #load data from gsc to bigquery
    gcs_bigquery = PythonOperator(
        task_id = "GCStoBigquery",
        python_callable = gcs_to_bigquery,
        op_kwargs=dict(
            bucket_name=BUCKET_NAME,
            file_path = FILE_PATH
        )
    )

    end = EmptyOperator(task_id="end")

    start >> download_file >> ingestion_data >> load_to_gcs >> gcs_bigquery >> delete_file >> end
    # start >> download_file >> ingestion_data  >> end