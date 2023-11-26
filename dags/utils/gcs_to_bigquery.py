import os
from google.cloud import bigquery


def gcs_to_bigquery(bucket_name, file_path):

    

    script_directory = os.path.dirname(os.path.abspath(__file__))
    relative_path = 'google/adeairflow-6fe91e99ed98.json'
    absolute_path = os.path.join(script_directory, relative_path)

    # BigQuery client
    client = bigquery.Client.from_service_account_json(absolute_path)
    print("********** got here ***************")
    # bucket and file path
    bucket_name = bucket_name # os.getenv['GCP_GCS_BUCKET']
    file_path = file_path  # os.getenv['GCP_file'] #'car_sale'
   
    # Specify BigQuery dataset and table
    dataset_id = os.getenv['DATA_SET']
    table_id = 'car_sale'

    # Construct the GCS URI
    uri = f'gs://{bucket_name}/{file_path}'

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Adjust based on your data file
        autodetect=True,  # Set to False if you have a schema defined
    )
    print("********* connected to bigquery succesfully  **************")
    # # Load data from GCS to BigQuery
    # load_job = client.load_table_from_uri(uri, dataset_id + '.' + table_id, job_config=job_config)

    # # Wait for the job to complete
    # load_job.result()

    # # Print the result
    # print(f'Loaded {load_job.output_rows} rows to {dataset_id}.{table_id}')
