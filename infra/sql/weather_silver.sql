INSERT INTO silver_weather(
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
        city,
        latitude,
        longitude,
        observation_time,
        temperature,
        relative_humidity,
        apparent_temperature,
        is_day,
        CASE
            WHEN is_day = 1 THEN "Day"
            WHEN is_day = 0 THEN "Night"
        END AS day_period,
        precipitation,
        rain,
        showers,
        wind_speed,
        wind_direction,
        surface_pressure,
        pressure_msl,
        cloud_cover, 
        weather_code,
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
            ELSE 'Unknown'
        END as weather_description
    FROM weather_data_bronze
    ON DUPLICATE KEY UPDATE
    temperature = VALUES(temperature),
    relative_humidity = VALUES(relative_humidity),
    apparent_temperature = VALUES(apparent_temperature),
    is_day = VALUES(is_day),
    day_period = VALUES(day_period),
    precipitation = VALUES(precipitation),
    rain = VALUES(rain),
    showers = VALUES(showers),
    wind_speed = VALUES(wind_speed),
    wind_direction = VALUES(wind_direction),
    surface_pressure = VALUES(surface_pressure),
    pressure_msl = VALUES(pressure_msl),
    cloud_cover = VALUES(cloud_cover),
    weather_code = VALUES(weather_code),
    weather_description = VALUES(weather_description);