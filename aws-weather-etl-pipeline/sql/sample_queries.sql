-- View sample data
SELECT * FROM weather_data LIMIT 20;

-- Highest temperatures
SELECT city, temperature
FROM weather_data
ORDER BY temperature DESC
LIMIT 10;

-- Average temperature per city
SELECT city, AVG(temperature) AS avg_temp
FROM weather_data
GROUP BY city
ORDER BY avg_temp DESC;

-- Humidity over 80%
SELECT *
FROM weather_data
WHERE humidity > 80;

-- Weather frequency by description
SELECT description, COUNT(*)
FROM weather_data
GROUP BY description
ORDER BY COUNT(*) DESC;

-- Convert timestamp from text to proper timestamp
SELECT
    city,
    temperature,
    humidity,
    description,
    TO_TIMESTAMP(timestamp, 'YYYY-MM-DD HH24:MI:SS') AS ts_converted
FROM weather_data

ORDER BY ts_converted DESC;
