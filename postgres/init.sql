CREATE TABLE weather_logs (
    id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    temperature FLOAT,
    pressure INT,
    humidity INT,
    weather_description VARCHAR(255),
    wind_speed FLOAT,
    clouds INT,
    date_fetched TIMESTAMP
);