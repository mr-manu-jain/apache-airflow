from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json


#Define DAG

with DAG(
    dag_id='NASA_apod_postgres',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    
    #Step1: Create Table if not exisits
    @task
    def create_table():
        ##initialise the postgres hooke to interact with postgres SQL
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")

        #SQL Query
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

        #Execute the table creation query
        postgres_hook.run(create_table_query)
        



# API KEY: iHfcIWPtR3gXk04Yaf3ehB2OQ5iIYHBkPSG8g35d
# END POINT: https://api.nasa.gov/planetary/apod?api_key=iHfcIWPtR3gXk04Yaf3ehB2OQ5iIYHBkPSG8g35d
        #Step2: Extract the NASA api Data (APOD) data
        extract_apod = SimpleHttpOperator(
             task_id='extract_apod',
             http_conn_id='nasa_api', ##Connection Id defined in Airflow for NASA API
             endpoint='planetray/apod', ## NASA API endpoint for APOD
             method='GET',
             data={"api_key": "{{conn.nasa_api.extra_dejson.api_key}}"}, #API KEY FROM CONNECTION
             response_filter= lambda response:response.json(),
        
        )   
   
#         ##Step3: Transform the data (Pick only the inofmration we need to save)
# API RESPONSE
#         {
#   "date": "2025-02-19",
#   "explanation": "How do stars and planets form? New clues have been found in the protoplanetary system Herbig-Haro 30 by the James Webb Space Telescope in concert with Hubble and the Earth-bound ALMA.  The observations show, among other things, that large dust grains are more concentrated into a central disk where they can form planets. The featured image from Webb shows many attributes of the active HH-30 system. Jets of particles are being expelled vertically, shown in red, while a dark dust-rich disk is seen across the center, blocking the light from the star or stars still forming there. Blue-reflecting dust is seen in a parabolic arc above and below the central disk, although why a tail appears on the lower left is currently unknown. Studying how planets form in HH 30 can help astronomers better understand how planets in our own Solar System once formed, including our Earth.",
#   "hdurl": "https://apod.nasa.gov/apod/image/2502/HH30_Webb_960.jpg",
#   "media_type": "image",
#   "service_version": "v1",
#   "title": "HH 30: A Star System with Planets Now Forming",
#   "url": "https://apod.nasa.gov/apod/image/2502/HH30_Webb_960.jpg"
# }

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

