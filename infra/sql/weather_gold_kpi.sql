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