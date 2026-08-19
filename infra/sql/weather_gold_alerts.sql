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