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