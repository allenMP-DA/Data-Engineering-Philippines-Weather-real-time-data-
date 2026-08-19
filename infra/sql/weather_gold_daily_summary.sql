 INSERT INTO GOLD_WEATHER_DAILY_SUMMARY
    SELECT
        city,
        DATE(observation_time),
        ROUND(AVG(temperature),2),
        MAX(temperature),
        MIN(temperature),
        ROUND(SUM(rain)2),
        ROUND(AVG(precipitation)2),
        ROUND(AVG(relative_humidity)2),
        ROUND(AVG(wind_speed)2)
    FROM silver_weather
    GROUP BY city, DATE(observation_time);