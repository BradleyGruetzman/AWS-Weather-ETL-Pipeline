-- Weather Data Table
CREATE TABLE IF NOT EXISTS weather (
    id SERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    temperature DOUBLE PRECISION,
    humidity BIGINT,
    description TEXT,
    timestamp TEXT
);