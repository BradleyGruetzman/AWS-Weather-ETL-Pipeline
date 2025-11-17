-- Weather Data Table
CREATE TABLE weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    temperature NUMERIC(5,2),
    feels_like NUMERIC(5,2),
    humidity INTEGER,
    pressure INTEGER,
    wind_speed NUMERIC(5,2),
    weather_main VARCHAR(50),
    weather_description VARCHAR(255),
    timestamp_utc TIMESTAMP,
    raw_json JSONB
);