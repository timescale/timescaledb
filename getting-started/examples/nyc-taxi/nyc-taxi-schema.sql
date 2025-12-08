-- TimescaleDB NYC Taxi Example Schema
--
-- This schema demonstrates optimal design for high-volume transportation data.
-- NYC Taxi data includes timestamps, locations, fares, and trip details.

-- Enable timing to show query performance
\timing on

-- Create the hypertable with optimal settings for NYC Taxi data
-- This automatically enables columnstore for fast analytical queries
CREATE TABLE trips (
    pickup_datetime TIMESTAMPTZ NOT NULL,
    dropoff_datetime TIMESTAMPTZ,
    passenger_count INTEGER,
    trip_distance DOUBLE PRECISION,
    pickup_location_id INTEGER,
    dropoff_location_id INTEGER,
    fare_amount DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    payment_type TEXT,
    cab_type TEXT
) WITH (
    tsdb.hypertable,
    tsdb.partition_column='pickup_datetime',
    tsdb.enable_columnstore=true,
    tsdb.segmentby='cab_type',
    tsdb.orderby='pickup_datetime DESC'
);

-- Create indexes for common query patterns
CREATE INDEX idx_trips_location_time
    ON trips (pickup_location_id, pickup_datetime DESC);

CREATE INDEX idx_trips_cabtype_time
    ON trips (cab_type, pickup_datetime DESC);

-- Add helpful table comments
COMMENT ON TABLE trips IS 'NYC Taxi trip data with automatic partitioning and columnstore compression';
COMMENT ON COLUMN trips.pickup_datetime IS 'Timestamp when the trip started';
COMMENT ON COLUMN trips.dropoff_datetime IS 'Timestamp when the trip ended';
COMMENT ON COLUMN trips.pickup_location_id IS 'NYC Taxi Zone ID for pickup location';
COMMENT ON COLUMN trips.dropoff_location_id IS 'NYC Taxi Zone ID for dropoff location';
COMMENT ON COLUMN trips.cab_type IS 'Type of cab (yellow, green, uber, etc.)';

\echo ''
\echo '=== NYC Taxi hypertable created successfully! ==='
\echo ''
\echo 'Table: trips'
\echo 'Features enabled:'
\echo '  - Automatic time-based partitioning'
\echo '  - Columnstore compression for fast analytics'
\echo '  - Optimized segmentation by cab_type'
\echo '  - Indexes for location and time-based queries'
\echo ''
\echo 'Next: Load sample data using nyc-taxi-sample.csv'
\echo ''
