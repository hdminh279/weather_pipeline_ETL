import requests
from datetime import datetime
import logging
from sqlalchemy import create_engine
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

API_KEY = os.getenv("OPENWEATHER_API_KEY")


def fetch_weather(api_key, lang="vi", units="metric"):
    base_url = "http://api.openweathermap.org/data/2.5/weather?"
    VN_CITIES = [
        {"city_name": "Ha Giang", "lat": "22.8233", "lon": "104.9833"},
        {"city_name": "Ha Noi", "lat": "21.0245", "lon": "105.8412"},
        {"city_name": "Hai Phong", "lat": "20.8651", "lon": "106.6838"},
        {"city_name": "Vinh", "lat": "18.6667", "lon": "105.6667"},
        {"city_name": "Hue", "lat": "16.4667", "lon": "107.5792"},
        {"city_name": "Da Nang", "lat": "16.0544", "lon": "108.2022"},
        {"city_name": "Quy Nhon", "lat": "13.7767", "lon": "109.2247"},
        {"city_name": "Da Lat", "lat": "11.9416", "lon": "108.4383"},
        {"city_name": "Ho Chi Minh City", "lat": "10.7756", "lon": "106.7019"},
        {"city_name": "Can Tho", "lat": "10.0452", "lon": "105.7469"}
    ]

    all_weather_data = []

    for city in VN_CITIES:
        try:
            complete_url = f"{base_url}lat={city['lat']}&lon={city['lon']}&lang={lang}&units={units}&appid={api_key}"

            response = requests.get(complete_url, timeout=10)
            response.raise_for_status()
            data = response.json()
            now = datetime.now()

            weather = {
                "city_name": city["city_name"],
                "temperature": data["main"]["temp"],
                "pressure": data["main"]["pressure"],
                "humidity": data["main"]["humidity"],
                "weather_description": data["weather"][0]["description"],
                "wind_speed": data["wind"]["speed"],
                "clouds": data["clouds"]["all"],
                "date_fetched": now.strftime("%Y-%m-%d %H:%M:%S")
            }

            all_weather_data.append(weather)
            logging.info(f"Fetched data for {city['city_name']} successfully.")

        except Exception as e:
            logging.error(f"Failed to fetch data for {city['city_name']}: {e}")
        
    return all_weather_data

def store_data():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    database = os.getenv("POSTGRES_DB")
    engine = create_engine(f"postgresql://{user}:{password}@postgres:5432/{database}")
    api_key = API_KEY

    data = fetch_weather(api_key)

    if not data:
        logging.warning("No data to store")
        return
    
    df = pd.DataFrame(data)

    try:
        df.to_sql(name='weather_logs', con=engine, if_exists="append", index=False, method='multi')
        logging.info("Push data to postgresql complete.")

    except Exception as e:
        logging.critical(f"Database error: {e}")


with DAG(
    dag_id="weather_etl",
    start_date=datetime(2026,3,4),
    schedule="@daily",
    catchup=False
) as dag:
    
    store_weather_task=PythonOperator(
        task_id="store_weather",
        python_callable=store_data
    )

    store_weather_task
