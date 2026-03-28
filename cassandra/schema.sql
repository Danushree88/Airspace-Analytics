CREATE KEYSPACE IF NOT EXISTS airspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE airspace;

CREATE TABLE IF NOT EXISTS flight_events (
    icao24 TEXT,
    timestamp TIMESTAMP,
    latitude DOUBLE,
    longitude DOUBLE,
    altitude DOUBLE,
    velocity DOUBLE,
    vertical_rate DOUBLE,
    country TEXT,
    region TEXT,
    airport_type TEXT,
    temperature DOUBLE,
    wind_speed DOUBLE,
    visibility DOUBLE,
    precipitation DOUBLE,
    aci DOUBLE,
    PRIMARY KEY (icao24, timestamp)
);