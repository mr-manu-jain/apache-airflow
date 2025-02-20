from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json

#Defining the DAG
with DAG(
    dag_id='NASA_apod_postgres',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
        
    #Step1: Creating Table if it doesn't exist
    @task
    def create_table():
        ##initialise the postgres hook to interact with postgres SQL
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")
        #SQL Query to create the table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        #Executing the table creation query
        postgres_hook.run(create_table_query)
                
    #Step2: Extract the NASA api Data (APOD) data
    extract_apod = SimpleHttpOperator(
             task_id='extract_apod',
             http_conn_id='nasa_api', ##Connection Id defined in Airflow for NASA API
             endpoint='planetary/apod', ## NASA API endpoint for APOD
             method='GET',
             data={"api_key": "{{conn.nasa_api.extra_dejson.api_key}}"}, #API KEY FROM CONNECTION
             response_filter= lambda response:response.json(),
        )   
   
    ##Step3: Transform the data (Pick only the inofmration we need to save)
    @task
    def transform_apod_data(response):
        apod_data = {
            'title': response.get('title',''),
            'explanation':response.get('explanation',''),
            'url':response.get('url',''),
            'date':response.get('date',''),
            'media_type':response.get('media_type','')
        }
        return apod_data
        
        
    #Step4: Loading into Postgres
    @task
    def load_data_to_postgres(apod_data):
        #Postgres hook
        postgres_hook=PostgresHook(postgres_conn_id='my_postgres_connection')
        #Insert Query to api data into postgres
        insert_query = """
            INSERT INTO apod_data (title,explanation,url,date,media_type)
            VALUES (%s,%s,%s,%s,%s);
        """
        postgres_hook.run(insert_query,parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))
    #Step5: Verify with the data DBViewer
    #Step6: Define the dependencies
    #Extract
    create_table() >> extract_apod
    api_response = extract_apod.output
    #Transform
    transformed_data = transform_apod_data(api_response)
    #Load
    load_data_to_postgres(transformed_data)