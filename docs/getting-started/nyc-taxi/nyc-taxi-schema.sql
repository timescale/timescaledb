-- TimescaleDB NYC Taxi Example Schema
--
-- This schema demonstrates optimal design for high-volume transportation data.
-- NYC Taxi data includes timestamps, locations, fares, and trip details.

-- Enable timing to show query performance
\timing on

-- Create the hypertable with optimal settings for NYC Taxi data
-- This automatically enables columnstore for fast analytical queries
CREATE TABLE trips (
    vendor_id TEXT,
    pickup_boroname VARCHAR,
    pickup_datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    dropoff_datetime TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    passenger_count NUMERIC,
    trip_distance NUMERIC,
    pickup_longitude NUMERIC,
    pickup_latitude NUMERIC,
    rate_code INTEGER,
    dropoff_longitude NUMERIC,
    dropoff_latitude NUMERIC,
    payment_type VARCHAR,
    fare_amount NUMERIC,
    extra NUMERIC,
    mta_tax NUMERIC,
    tip_amount NUMERIC,
    tolls_amount NUMERIC,
    improvement_surcharge NUMERIC,
    total_amount NUMERIC
) WITH (
    tsdb.hypertable,
    tsdb.partition_column='pickup_datetime',
    tsdb.enable_columnstore=true,
    tsdb.segmentby='pickup_boroname',
    tsdb.orderby='pickup_datetime DESC'
);

-- Create indexes for common query patterns on rowstore data
-- Note: These indexes primarily help with uncompressed rowstore data.
-- Columnstore queries use internal structures (min/max stats) for pruning.
CREATE INDEX idx_trips_pickup_time ON trips (pickup_datetime DESC);
CREATE INDEX idx_trips_borough_time ON trips (pickup_boroname, pickup_datetime DESC);

-- Add helpful table comments
COMMENT ON TABLE trips IS 'NYC Taxi trip data with automatic partitioning and columnstore compression';
COMMENT ON COLUMN trips.vendor_id IS 'Taxi vendor ID';
COMMENT ON COLUMN trips.pickup_boroname IS 'Pickup borough name (Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR)';
COMMENT ON COLUMN trips.pickup_datetime IS 'Timestamp when the trip started';
COMMENT ON COLUMN trips.dropoff_datetime IS 'Timestamp when the trip ended';
COMMENT ON COLUMN trips.passenger_count IS 'Number of passengers';
COMMENT ON COLUMN trips.trip_distance IS 'Trip distance in miles';
COMMENT ON COLUMN trips.pickup_longitude IS 'Pickup location longitude';
COMMENT ON COLUMN trips.pickup_latitude IS 'Pickup location latitude';
COMMENT ON COLUMN trips.rate_code IS 'Rate code for the trip';
COMMENT ON COLUMN trips.dropoff_longitude IS 'Dropoff location longitude';
COMMENT ON COLUMN trips.dropoff_latitude IS 'Dropoff location latitude';
COMMENT ON COLUMN trips.payment_type IS 'Payment type (e.g., Credit card, Cash, No charge, Dispute, Unknown, Voided trip)';
COMMENT ON COLUMN trips.fare_amount IS 'Base fare amount';
COMMENT ON COLUMN trips.extra IS 'Extra charges';
COMMENT ON COLUMN trips.mta_tax IS 'MTA tax';
COMMENT ON COLUMN trips.tip_amount IS 'Tip amount';
COMMENT ON COLUMN trips.tolls_amount IS 'Tolls amount';
COMMENT ON COLUMN trips.improvement_surcharge IS 'Improvement surcharge';
COMMENT ON COLUMN trips.total_amount IS 'Total trip amount';

\echo ''
\echo '=== NYC Taxi hypertable created successfully! ==='
\echo ''
\echo 'Table: trips'
\echo 'Features enabled:'
\echo '  - Automatic time-based partitioning'
\echo '  - Columnstore compression for fast analytics'
\echo '  - Optimized segmentation by pickup_boroname (6 boroughs)'
\echo ''
\echo 'Next: Load sample data using nyc-taxi-sample.csv'
\echo ''
