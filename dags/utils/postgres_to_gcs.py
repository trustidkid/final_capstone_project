import psycopg2
from google.cloud import storage
import os

def to_gcs(user, password, host, db, table_name ):
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host= host,
        database=db,
        user= user,
        password=password
    )

    # Execute SQL query to extract data
    query = "SELECT * FROM " + table_name
    cursor = conn.cursor()
    cursor.execute(query)

    # Fetch the data
    data = cursor.fetchall()

    script_directory = os.path.dirname(os.path.abspath(__file__))
    relative_path = 'google/adeairflow-6fe91e99ed98.json'
    absolute_path = os.path.join(script_directory, relative_path)

    # Set up GCS client
    client = storage.Client.from_service_account_json(absolute_path)

    # GCS bucket and destination file name
    bucket_name = os.getenv('GCP_GCS_BUCKET')
    blob_name = os.getenv('GCP_FILE')

    # Upload data to GCS
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string('\n'.join([','.join(map(str, row)) for row in data]))
    
    # Close the connection
    conn.close()
    
    print("Data loaded successfully to GSC Bucket { }" + bucket_name)
