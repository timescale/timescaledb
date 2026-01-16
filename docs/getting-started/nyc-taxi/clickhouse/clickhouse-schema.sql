-- ClickHouse NYC Taxi Example Schema
--
-- This schema demonstrates NYC Taxi data modeling in ClickHouse.
-- ClickHouse uses columnar storage by default.

-- Create the trips table with MergeTree engine
CREATE TABLE trips (
    vendor_id String,
    pickup_boroname String,
    pickup_datetime DateTime,
    dropoff_datetime DateTime,
    passenger_count Decimal(18, 2),
    trip_distance Decimal(18, 2),
    pickup_longitude Decimal(18, 10),
    pickup_latitude Decimal(18, 10),
    rate_code Int32,
    dropoff_longitude Decimal(18, 10),
    dropoff_latitude Decimal(18, 10),
    payment_type String,
    fare_amount Decimal(18, 2),
    extra Decimal(18, 2),
    mta_tax Decimal(18, 2),
    tip_amount Decimal(18, 2),
    tolls_amount Decimal(18, 2),
    improvement_surcharge Decimal(18, 2),
    total_amount Decimal(18, 2)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(pickup_datetime)
ORDER BY (pickup_boroname, pickup_datetime);

-- Note: ClickHouse uses columnar storage by default with the MergeTree engine
-- The PARTITION BY clause partitions data by month
-- The ORDER BY clause determines the primary key and sorting order
