import pandas as pd
import json
import requests
from kafka import KafkaProducer

# API = api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,relative_humidity_2m,apparent_temperature,is_day,precipitation,rain,showers,wind_speed_10m,wind_direction_10m,surface_pressure,pressure_msl,cloud_cover,weather_code&minutely_15=temperature_2m,rain,snowfall,is_day
# API 2 = https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&hourly=temperature_2m,relative_humidity_2m,apparent_temperature,rain,cloud_cover,weather_code,wind_speed_10m,wind_direction_10m,wind_gusts_10m,visibility
BASE_URL = "https://api.open-meteo.com/v1/forecast?"

producer = KafkaProducer(
    bootstrap_servers=["host.docker.internal:29092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)


def load_locations():
    return pd.read_csv(
        r"D:\AllenMartinezPicazo\Certificates, Portfolio and Projects\Projects\Real Datasets Project\Projects\real-time-weather-philippines-update\data\ph.csv"
    )


def fetch_data(lat, lon):
    url = (
        f"{BASE_URL}"
        f"latitude={lat}"
        f"&longitude={lon}"
        "&current="
        "temperature_2m,"
        "relative_humidity_2m,"
        "apparent_temperature,"
        "is_day,"
        "precipitation,"
        "rain,"
        "showers,"
        "wind_speed_10m,"
        "wind_direction_10m,"
        "surface_pressure,"
        "pressure_msl,"
        "cloud_cover,"
        "weather_code"
        "&hourly="
        "temperature_2m,"
        "relative_humidity_2m,"
        "apparent_temperature,"
        "rain,"
        "cloud_cover,"
        "weather_code,"
        "is_day,"
        "wind_speed_10m"
    )

    response = requests.get(url)
    response.raise_for_status()

    return response.json()


locations = load_locations()

for _, row in locations.iterrows():
    data = fetch_data(row["lat"], row["lng"])
    data["city"] = row["city"]

    producer.send("weather-topic", data)

    print(f"Sent weather data for {row['city']}")
