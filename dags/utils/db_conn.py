import os
from time import time

from sqlalchemy import create_engine
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd

def db_connection(db, table_name, json_file, execution_date):

    print("print out result", db, json_file, execution_date)

    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')  # Specify your connection ID

    connection_info = postgres_hook.get_connection("postgres_conn_id")
    engine = create_engine(f"postgresql://{connection_info.login}:{connection_info.password}@{connection_info.host}:{connection_info.port}/{connection_info.schema}")

    conn = engine.connect()


    # Get a connection
    # conn = postgres_hook.get_conn()
    # cursor = conn.cursor()
    #create a table
    # ceate_table = "create table "

    # Define your SQL query
    # sql_query = "SELECT * FROM your_table;"

    # # Execute the SQL query using the hook
    # result = postgres_hook.get_records(sql_query)

    # Process the result as needed
    # for row in result:
    #     print(row)

    # engine = create_engine("postgresql://{user}:{password}@{host}:{port}/{db}")

    # engine.connect()

    print("connection was successful")

    start_time = time()
    # read_df = pd.read_json(json_file,encoding='utf-8', encoding="unicode_escape", iterator=True,  chunksize=100000)
    # read_df = pd.read_json(json_file, encoding='unicode_escape', lines=True)
    # print("***** Display the columns ******")
    # print(read_df.head())

    
    # Read JSON file in chunks
    chunk_size = 100000
    disaster_df = pd.read_json(json_file, encoding='unicode_escape', lines=True, chunksize=chunk_size )

    # print(car_list.head())
    # df = next(disaster_df)

    # print(car_list)
    # print(disaster_df.columns)

    #Creates a table with certain headers only the column headers
    # disaster_df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    # print(disaster_df['disaster_number'])
    for chunk in disaster_df:
        print(chunk['disaster_number'])

    # disaster_df.to_sql(name=table_name, con=engine, if_exists='replace')

    # while True:
    #     start_time = time()

    #     try:
    #     #    df = next(disaster_df)

    #        disaster_df.to_sql(name=table_name, con=engine, if_exists='append')

    #        end_time = time()
    #        print("Insertion for this chunk was successfull at complete at about %.3f seconds" % (end_time - start_time))
    #     except StopIteration:
    #        print('Loading has ended')
    #     break

    # for chunk in pd.read_csv(json_file, encoding='unicode_escape', iterator=True, error_bad_lines=False, chunksize=chunk_size ):
    #     # print(f"Processing chunk {i + 1}")
    #     # Process each chunk or write it directly to the database
    #     # print("Json file read successfully")
        
    #     try:        
    #         chunk.to_sql(table_name, con=engine, index=False, if_exists='append')
    #         # chunk.to_sql(table_name, con=postgres_hook.get_sqlalchemy_engine(), index=False, if_exists='replace')
    #     except StopIteration:
    #         print("***Loading ended")
    #         break



    #     conn.close()
    #     print("Data written successfully ")
    # df = next(read_df)

    # # Change the two date_time columns into actual date_time objects
    # df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    # df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    #Creates a table with certain headers only the column headers
    # read_df.head(n=0).to_sql(name=table_name, con=postgres_hook.get_sqlalchemy_engine(), if_exists='replace')

    # read_df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

    # Write the DataFrame to the PostgreSQL database
    # read_df.to_sql(table_name, con=postgres_hook.get_sqlalchemy_engine(), index=False, if_exists='replace')

    print("table created successfully")


    # while True:

    #    start_time = time()

    #    try:
    #     #    df = next(df_iter)

    #        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    #        df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

    #        end_time = time()
    #        print("Insertion for this chunk was successfull at complete at about %.3f seconds" % (end_time - start_time))

    #    except StopIteration:
    #        print('Loading has ended')
    #        break