INSERT IGNORE INTO GOLD_WEATHER_LOCATION
    SELECT DISTINCT
        city,
        latitude,
        longitude
    FROM silver_weather;