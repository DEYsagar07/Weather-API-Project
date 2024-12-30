from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor #read somthing from API
from airflow.operators.python import PythonOperator #run python code
from airflow.providers.postgres.hooks.postgres import PostgresHook #pusing data inside db
from airflow.providers.http.operators.http import SimpleHttpOperator
#from airflow.decorators import task
#from airflow.utils.dates import days_ago
import requests
import json

POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'weathermap_api'

def response_filter(response):
     if response.status_code == 200:
          return response.json()
     else:
          raise Exception (f"Failed to fetch weather data: {response.status_code}")


def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids = "extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit = kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure =  data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.fromtimestamp(data['dt']+data['timezone'])
    sunrise_time = datetime.fromtimestamp(data['sys']['sunrise']+data['timezone'])
    sunset_time = datetime.fromtimestamp(data['sys']['sunset']+data['timezone'])

    transform_data = {"City":city,
                      "Description":weather_description,
                      "Temperature (F)":temp_farenheit,
                      "Feels Like (F)": feels_like_farenheit,
                      "Minimum Temp (F)":min_temp_farenheit,
                      "Maximum Temp (F)":max_temp_farenheit,
                      "Pressure":pressure,
                      "Humidity":humidity,
                      "Wind Speed": wind_speed,
                      "Time_of_Record":time_of_record,
                      "Sunrise (Local Time)":sunrise_time,
                      "Sunset (Local Time)":sunset_time,
                      }
    #push data to postgress table 
    pg_hook = PostgresHook(postgress_conn_id=POSTGRES_CONN_ID)
    #table_name = "weather_data"
    #pg_hook.insert_rows(table=table_name, rows=[transform_data])
    #transformed_data_list = [transform_data]
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
     # Create table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS weather_data (
        city VARCHAR,
        description VARCHAR,
        temperature FLOAT,
        feels_like FLOAT,
        min_temp FLOAT,
        max_temp FLOAT,
        pressure INT,
        humidity INT,
        wind_speed FLOAT,
        time_of_record TIMESTAMP,
        sunrise TIMESTAMP,
        sunset TIMESTAMP
    );
    """)

    # Insert transformed data into the table
    cursor.execute("""
    INSERT INTO weather_data (city, description, temperature, feels_like, min_temp, max_temp, pressure, humidity, wind_speed, time_of_record, sunrise, sunset)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        transform_data['City'],
        transform_data['Description'],
        transform_data['Temperature (F)'],
        transform_data['Feels Like (F)'],
        transform_data['Minimum Temp (F)'],
        transform_data['Maximum Temp (F)'],
        transform_data['Pressure'],
        transform_data['Humidity'],
        transform_data['Wind Speed'],
        transform_data['Time_of_Record'],
        transform_data['Sunrise (Local Time)'],
        transform_data['Sunset (Local Time)']
    ))

    conn.commit()
    cursor.close()

default_args = {
    'owner':'airflow',
    'depends_on_past': False,
    'start_date':datetime(2024,10,27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
   # 'retry_delay': timedelta(minutes=5),

}


with DAG(dag_id='weather_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

        is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='API_CONN_ID',
        endpoint='/data/2.5/weather?q=Durgapur&APPID=bb7c265893d4a50f4fc8cf9bb48fc7d3'
        )

        extract_weather_data = SimpleHttpOperator(
        task_id='extract_weather_data',
        http_conn_id='API_CONN_ID',
        endpoint='/data/2.5/weather?q=Durgapur&APPID=bb7c265893d4a50f4fc8cf9bb48fc7d3',
        method='GET',
        #response_filter=lambda response: json.loads(response.text)
        response_filter = response_filter
        )

        transform_load_weather_data = PythonOperator(
        task_id='transform_load_weather_data',
        python_callable =  transform_load_data
        )

        load_weather_data_to_postgres = PythonOperator(
        task_id='load_weather_data_to_postgres',
        python_callable=transform_load_data
        )

        is_weather_api_ready >> extract_weather_data >> transform_load_weather_data >> load_weather_data_to_postgres

