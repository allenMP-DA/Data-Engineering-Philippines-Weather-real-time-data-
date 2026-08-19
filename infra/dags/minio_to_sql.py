import json
import os
import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from datetime import datetime

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = " "
MINIO_SECRET_KEY = " "
BUCKET = "weather-update-bucket"
LOCAL_DIR = "/tmp/minio_downloads/weather"
PREFIX = ""
MYSQL_CONN = "mysql+pymysql://user:password@mysql:3306/weather_db"


# Extract from MINIO
def extract_from_minio(prefix, local_dir):
    os.makedirs(local_dir, exist_ok=True)

    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET, Prefix=prefix)

    local_files = []

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(".json"):
                continue

            # Preserve the folder structure (city folders)
            local_file = os.path.join(local_dir, key)
            # "/tmp/weather_data"+"weather/2026/07/27/manila.json"
            os.makedirs(os.path.dirname(local_file), exist_ok=True)
            # 1 os.path.dirname(local_file) >> /tmp/minio_downloads/weather/2026/07/27/Manila
            # 2 os.makedirs("/tmp/minio_downloads/weather/2026/07/27/Manila",exist_ok=True   >> creates all missing folders. Only folder
            # Final output  >> #/tmp/minio_downloads/weather/2026/07/27/Manila/weather_20260727_1115.json
            # Always download the latest copy
            s3.download_file(BUCKET, key, local_file)

            print(f"Downloaded {key} -> {local_file}")

            local_files.append(local_file)

    print(f"Downloaded {len(local_files)} files")
    return local_files


# -----------------------------
# TRANSFORM WEATHER
# -----------------------------
def transform_weather(ti):
    files = ti.xcom_pull(task_ids="extract_weather")

    print("Files received:")
    print(files)

    records = []

    for file in files:
        print(f"Reading {file}")

        with open(file, "r") as f:
            data = json.load(f)

        print("Keys:", list(data.keys()))

        # -----------------------------
        # LOCATION
        # -----------------------------
        city = data.get("city")
        latitude = data.get("latitude")
        longitude = data.get("longitude")

        print("City:", city)
        print("Latitude:", latitude)
        print("Longitude:", longitude)

        if not city or latitude is None or longitude is None:
            print("Skipping file - missing location information")
            continue

        # ==================================================
        # CURRENT WEATHER
        # ==================================================
        current = data.get("current")

        if current:
            records.append(
                {
                    "city": city,
                    "latitude": latitude,
                    "longitude": longitude,
                    "observation_time": current.get("time"),
                    "temperature": current.get("temperature_2m"),
                    "relative_humidity": current.get("relative_humidity_2m"),
                    "apparent_temperature": current.get("apparent_temperature"),
                    "is_day": current.get("is_day"),
                    "precipitation": current.get("precipitation"),
                    "rain": current.get("rain"),
                    "showers": current.get("showers"),
                    "wind_speed": current.get("wind_speed_10m"),
                    "wind_direction": current.get("wind_direction_10m"),
                    "surface_pressure": current.get("surface_pressure"),
                    "pressure_msl": current.get("pressure_msl"),
                    "cloud_cover": current.get("cloud_cover"),
                    "weather_code": current.get("weather_code"),
                }
            )

            print(f"Current data added for {city}")

        # ==================================================
        # 15-MINUTE HISTORICAL DATA
        # ==================================================
        hourly = data.get("hourly")

        if hourly:
            times = hourly.get("time", [])

            print(f"hourly records found for {city}: {len(times)}")

            for i, timestamp in enumerate(times):

                def get_value(field):
                    values = hourly.get(field, [])
                    return values[i] if i < len(values) else None

                records.append(
                    {
                        "city": city,
                        "latitude": latitude,
                        "longitude": longitude,
                        "observation_time": timestamp,
                        "temperature": get_value("temperature_2m"),
                        # Historical JSON doesn't contain these
                        # fields, so they will be NULL
                        "relative_humidity": get_value("relative_humidity_2m"),
                        "apparent_temperature": get_value("apparent_temperature"),
                        "is_day": get_value("is_day"),
                        "precipitation": None,
                        "rain": get_value("rain"),
                        "showers": None,
                        "wind_speed": get_value("wind_speed_10m"),
                        "wind_direction": get_value("wind_direction_10m"),
                        "surface_pressure": None,
                        "pressure_msl": None,
                        "cloud_cover": get_value("cloud_cover"),
                        "weather_code": get_value("weather_code"),
                    }
                )

        print(f"Finished {city}")

    print(f"Total records created: {len(records)}")

    return records


# -----------------------------
# LOAD WEATHER
# -----------------------------
def load_weather(ti):
    records = ti.xcom_pull(task_ids="transform_weather")

    print("Records received:")
    print(records)

    if not records:
        print("No records received from transform_weather")
        return

    df = pd.DataFrame(records)

    print(df.head())
    print(df.shape)

    engine = create_engine(MYSQL_CONN)

    df.to_sql(
        "weather_data_bronze",
        con=engine,
        if_exists="append",
        index=False,
    )

    print("Weather loaded successfully!")


## -------
# SILVER WEATHER TABLE
## -------


def load_silver_weather():
    engine = create_engine(MYSQL_CONN)

    query = """
    INSERT IGNORE INTO silver_weather(
        city,
        latitude,
        longitude,
        observation_time,
        temperature,
        relative_humidity,
        apparent_temperature,
        is_day,
        day_period,
        precipitation,
        rain,
        showers,
        wind_speed,
        wind_direction,
        surface_pressure,
        pressure_msl,
        cloud_cover,
        weather_code,
        weather_description
    )
    SELECT
        b.city,
        b.latitude,
        b.longitude,
        b.observation_time,
        b.temperature,
        b.relative_humidity,
        b.apparent_temperature,
        b.is_day,

        CASE
            WHEN b.is_day = 1 THEN 'Day'
            WHEN b.is_day = 0 THEN 'Night'
            ELSE NULL
        END AS day_period,

        b.precipitation,
        b.rain,
        b.showers,
        b.wind_speed,
        b.wind_direction,
        b.surface_pressure,
        b.pressure_msl,
        b.cloud_cover,
        b.weather_code,

        CASE
            WHEN b.weather_code = 0 THEN 'Clear sky'
            WHEN b.weather_code IN (1,2,3) THEN 'Mainly clear, Partly cloudy, Overcast'
            WHEN b.weather_code IN (45,48) THEN 'Fog'
            WHEN b.weather_code IN (51,53,55) THEN 'Drizzle'
            WHEN b.weather_code IN (56,57) THEN 'Freezing Drizzle'
            WHEN b.weather_code IN (61,63,65) THEN 'Rain'
            WHEN b.weather_code IN (66,67) THEN 'Freezing Rain'
            WHEN b.weather_code IN (71,73,75) THEN 'Snowfall'
            WHEN b.weather_code = 77 THEN 'Snow Grains'
            WHEN b.weather_code IN (80,81,82) THEN 'Rain Showers'
            WHEN b.weather_code IN (85,86) THEN 'Snow Showers'
            WHEN b.weather_code = 95 THEN 'Thunderstorm'
            WHEN b.weather_code IN (96,99) THEN 'Thunderstorm with Hail'
            ELSE 'Unknown'
        END AS weather_description

    FROM weather_data_bronze b
    """

    with engine.begin() as conn:
        conn.execute(text(query))

    print("Silver Weather loaded successfully")


def gold_weather_kpi():
    engine = create_engine(MYSQL_CONN)
    query = """
    INSERT INTO GOLD_WEATHER_KPI
    SELECT
        city,
        temperature,
        apparent_temperature as feels_like,
        relative_humidity as humidity,
        rain,
        wind_speed,
        weather_description as weather_status,
        day_period,
        observation_time as last_updated
    FROM(
        SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY city
            ORDER BY observation_time DESC
        )AS rn
        FROM silver_weather
    ) t
    WHERE rn = 1;
    """

    with engine.begin() as conn:
        conn.execute(text(query))


def gold_weather_daily_summary():
    engine = create_engine(MYSQL_CONN)
    query = """
    INSERT INTO GOLD_WEATHER_DAILY_SUMMARY
    SELECT
        city,
        DATE(observation_time),
        ROUND(AVG(temperature),2),
        MAX(temperature),
        MIN(temperature),
        ROUND(SUM(rain),2),
        ROUND(AVG(precipitation),2),
        ROUND(AVG(relative_humidity),2),
        ROUND(AVG(wind_speed),2)
    FROM silver_weather
    WHERE precipitation IS NOT NULL
    GROUP BY city, DATE(observation_time);
    """

    with engine.begin() as conn:
        conn.execute(text(query))


def gold_weather_trend():
    engine = create_engine(MYSQL_CONN)
    query = """
    INSERT INTO GOLD_WEATHER_TREND
    SELECT
        city,
        observation_time,
        temperature,
        apparent_temperature,
        relative_humidity,
        rain,
        precipitation,
        wind_speed,
        cloud_cover
    FROM silver_weather;
    """
    with engine.begin() as conn:
        conn.execute(text(query))


def gold_weather_alerts():
    engine = create_engine(MYSQL_CONN)
    query = """
    INSERT INTO GOLD_WEATHER_ALERTS
    SELECT
    city,
    observation_time,
    CASE
        WHEN weather_code = 0 THEN 'Clear sky'
        WHEN weather_code IN (1,2,3) THEN 'Mainly clear, Partly cloudy, Overcast'
        WHEN weather_code IN (45,48) THEN 'Fog'
        WHEN weather_code IN (51,53,55) THEN 'Drizzle'
        WHEN weather_code IN (56,57) THEN 'Freezing Drizzle'
        WHEN weather_code IN (61,63,65) THEN 'Rain'
        WHEN weather_code IN (66,67) THEN 'Freezing Rain'
        WHEN weather_code IN (71,73,75) THEN 'Snowfall'
        WHEN weather_code = 77 THEN 'Snow Grains'
        WHEN weather_code IN (80,81,82) THEN 'Rain Showers'
        WHEN weather_code IN (85,86) THEN 'Snow Showers'
        WHEN weather_code = 95 THEN 'Thunderstorm'
        WHEN weather_code IN (96,99) THEN 'Thunderstorm with Hail'
        WHEN rain >= 10 THEN 'Heavy Rain'
        WHEN wind_speed >= 40 THEN 'Strong Wind'
        WHEN temperature >= 35 THEN 'Extreme Heat'
        WHEN cloud_cover >= 90 THEN 'Overcast'
        ELSE 'Normal'
        END AS alert
    FROM silver_weather;
    """
    with engine.begin() as conn:
        conn.execute(text(query))


def gold_weather_location():
    engine = create_engine(MYSQL_CONN)
    query = """
    INSERT IGNORE INTO GOLD_WEATHER_LOCATION
    SELECT DISTINCT
        city,
        latitude,
        longitude
    FROM silver_weather;
    """
    with engine.begin() as conn:
        conn.execute(text(query))


# ----------------------------------
# DAG
# ----------------------------------
with DAG(
    dag_id="weather_data_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    weather_extract = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_from_minio,
        op_kwargs={"prefix": PREFIX, "local_dir": LOCAL_DIR},
    )
    weather_transform = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather,
    )
    weather_load = PythonOperator(
        task_id="load_weather",
        python_callable=load_weather,
    )
    weather_silver = PythonOperator(
        task_id="silver_weather",
        python_callable=load_silver_weather,
    )
    weather_gold_alerts = PythonOperator(
        task_id="gold_weather_alerts",
        python_callable=gold_weather_alerts,
    )
    weather_gold_daily_summary = PythonOperator(
        task_id="gold_weather_daily_summary",
        python_callable=gold_weather_daily_summary,
    )
    weather_gold_kpi = PythonOperator(
        task_id="gold_weather_kpi",
        python_callable=gold_weather_kpi,
    )
    weather_gold_location = PythonOperator(
        task_id="gold_weather_location",
        python_callable=gold_weather_location,
    )
    weather_gold_trend = PythonOperator(
        task_id="gold_weather_trend",
        python_callable=gold_weather_trend,
    )

    (
        weather_extract
        >> weather_transform
        >> weather_load
        >> weather_silver
        >> [
            weather_gold_alerts,
            weather_gold_daily_summary,
            weather_gold_kpi,
            weather_gold_location,
            weather_gold_trend,
        ]
    )
