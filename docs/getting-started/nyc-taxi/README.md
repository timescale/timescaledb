# NYC Taxi Data Example

Get started with TimescaleDB using New York City taxi trip data. This example demonstrates how to handle high-volume transportation data with location-based analytics and time-series aggregations.

## What You'll Learn

- How to model high-volume transportation data with lat/lon coordinates
- Time-series aggregations with `time_bucket()`
- Optimal segmentation strategies for compression
- Revenue and usage pattern analysis
- Loading data with direct to columnstore for instant performance

## Prerequisites

- Docker installed
- `psql` PostgreSQL client
- 15-20 minutes

## Quick Start

### Step 1: Start TimescaleDB

You have two options to start TimescaleDB:

#### Option 1: One-line install (Recommended)

The easiest way to get started:

> **Important:** This script is intended for local development and testing only. Do **not** use it for production deployments. For production-ready installation options, see the [TimescaleDB installation guide](https://docs.timescale.com/self-hosted/latest/install/).

**Linux/Mac:**

```sh
curl -sL https://tsdb.co/start-local | sh
```

This command:
- Downloads and starts TimescaleDB (if not already downloaded)
- Exposes PostgreSQL on port **6543** (a non-standard port to avoid conflicts with other PostgreSQL instances on port 5432)
- Automatically tunes settings for your environment using timescaledb-tune
- Sets up a persistent data volume

#### Option 2: Manual Docker command also used for Windows

Alternatively, you can run TimescaleDB directly with Docker:

```bash
docker run -d --name timescaledb \
    -p 6543:5432 \
    -e POSTGRES_PASSWORD=password \
    timescale/timescaledb-ha:pg18
```

**Note:** We use port **6543** (mapped to container port 5432) to avoid conflicts if you have other PostgreSQL instances running on the standard port 5432.

Wait about 1-2 minutes for TimescaleDB to download & initialize.

### Step 2: Connect to TimescaleDB

Connect using `psql`:

```bash
psql -h localhost -p 6543 -U postgres
# When prompted, enter password: password
```

You should see the PostgreSQL prompt. Verify TimescaleDB is installed:

```sql
SELECT extname, extversion FROM pg_extension WHERE extname = 'timescaledb';
```

Expected output:
```
   extname   | extversion
-------------+------------
 timescaledb | 2.x.x
```

**Prefer a GUI?** If you'd rather use a graphical tool instead of the command line, you can download [pgAdmin](https://www.pgadmin.org/download/) and connect to TimescaleDB using the same connection details (host: `localhost`, port: `6543`, user: `postgres`, password: `password`).

### Step 3: Create the Schema

Create the optimized hypertable by running this SQL in your `psql` session:

```sql
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

-- Create indexes 
CREATE INDEX idx_trips_pickup_time ON trips (pickup_datetime DESC);
CREATE INDEX idx_trips_borough_time ON trips (pickup_boroname, pickup_datetime DESC);
```

This creates a `trips` table with:
- Automatic time-based partitioning on `pickup_datetime`
- Columnstore enabled for fast analytical queries
- Segmentation by `pickup_boroname` for optimal compression (6 boroughs)
- Full trip details including fares, distances, and coordinates

### Step 4: Load Sample Data

First, download and decompress the sample data:

```bash
# Download the sample data
wget https://assets.timescale.com/timescaledb-datasets/nyc_taxi_sample_nov_dec_2015.csv.gz

# Decompress the CSV file
gunzip nyc_taxi_sample_nov_dec_2015.csv.gz

# This will create nyc_taxi_sample_nov_dec_2015.csv ready for loading
```

We provide two approaches for loading data. Choose based on your needs:

#### Option A: Direct to Columnstore (Recommended - Instant Performance)

This approach writes data directly to the columnstore, bypassing the rowstore entirely. You get instant analytical performance.

**From command line:**

```bash
psql -h localhost -p 6543 -U postgres \
  -v ON_ERROR_STOP=1 \
  -c "SET timescaledb.enable_direct_compress_copy = on;
      COPY trips FROM STDIN WITH (FORMAT csv, HEADER true);" \
  < nyc_taxi_sample_nov_dec_2015.csv
```

This command reads the CSV file from your local filesystem and pipes it to PostgreSQL, which loads it directly into the columnstore.

**Verify data loaded:**

```sql
SELECT COUNT(*) FROM trips;
```

#### Option B: Standard COPY (Fallback)

This approach loads data into the rowstore first. Data will be converted to the columnstore by a background policy (12-24 hours) for faster querying.

**From command line:**

```bash
psql -h localhost -p 6543 -U postgres \
  -v ON_ERROR_STOP=1 \
  -c "COPY trips FROM STDIN WITH (FORMAT csv, HEADER true);" \
  < nyc_taxi_sample_nov_dec_2015.csv
```

**Verify data loaded:**

```sql
SELECT COUNT(*) FROM trips;
```

**Manually convert to columnstore (Optional):**

If you loaded data using standard copy a background process will convert your rowstore data to the columnstore in 12-24 hours, you can manually convert it immediately to get the best query performance:

```sql
DO $$
DECLARE ch TEXT;
BEGIN
    FOR ch IN SELECT show_chunks('trips') LOOP
        CALL convert_to_columnstore(ch);
    END LOOP;
END $$;
```

### Step 5: Run Sample Queries

Now let's explore the data with some analytical queries. Run these in your `psql` session:

**Query 1: Overall statistics**
```sql
\timing on

SELECT
    COUNT(*) as total_trips,
    ROUND(SUM(fare_amount)::numeric, 2) as total_revenue,
    ROUND(AVG(fare_amount)::numeric, 2) as avg_fare,
    ROUND(AVG(trip_distance)::numeric, 2) as avg_distance
FROM trips;
```

**Query 2: Breakdown by vendor**
```sql
SELECT
    vendor_id,
    COUNT(*) as trips,
    ROUND(AVG(fare_amount)::numeric, 2) as avg_fare,
    ROUND(AVG(tip_amount)::numeric, 2) as avg_tip,
    ROUND(AVG(passenger_count)::numeric, 2) as avg_passengers
FROM trips
GROUP BY vendor_id
ORDER BY trips DESC;
```

**Query 3: Hourly patterns using time_bucket**
```sql
SELECT
    time_bucket('1 hour', pickup_datetime) AS hour,
    COUNT(*) as trips,
    ROUND(AVG(fare_amount)::numeric, 2) as avg_fare,
    ROUND(SUM(tip_amount)::numeric, 2) as total_tips
FROM trips
GROUP BY hour
ORDER BY hour DESC
LIMIT 24;
```

**Query 4: Payment type analysis**
```sql
SELECT
    payment_type,
    COUNT(*) as trip_count,
    ROUND(SUM(fare_amount)::numeric, 2) as total_revenue,
    ROUND(AVG(trip_distance)::numeric, 2) as avg_distance,
    ROUND(AVG(tip_amount)::numeric, 2) as avg_tip
FROM trips
GROUP BY payment_type
ORDER BY total_revenue DESC;
```

**Query 5: Daily statistics by borough**
```sql
SELECT
    time_bucket('1 day', pickup_datetime) AS day,
    pickup_boroname,
    COUNT(*) as trips,
    ROUND(AVG(fare_amount)::numeric, 2) as avg_fare,
    ROUND(MAX(fare_amount)::numeric, 2) as max_fare
FROM trips
GROUP BY day, pickup_boroname
ORDER BY day DESC, pickup_boroname
LIMIT 20;
```

**Query 6: Trips by distance category**
```sql
SELECT
    CASE
        WHEN trip_distance < 1 THEN 'Short (< 1 mile)'
        WHEN trip_distance < 5 THEN 'Medium (1-5 miles)'
        WHEN trip_distance < 10 THEN 'Long (5-10 miles)'
        ELSE 'Very Long (> 10 miles)'
    END as distance_category,
    COUNT(*) as trips,
    ROUND(AVG(fare_amount)::numeric, 2) as avg_fare,
    ROUND(AVG(tip_amount)::numeric, 2) as avg_tip
FROM trips
GROUP BY distance_category
ORDER BY trips DESC;
```

## What's Happening Behind the Scenes?

### Hypertables
When you create a table with `tsdb.hypertable`, TimescaleDB automatically:
- Partitions your data into time-based chunks (default: 7 days per chunk)
- Manages chunk lifecycle automatically
- Optimizes queries to scan only relevant chunks

### Columnstore Compression
With `tsdb.enable_columnstore=true`:
- Data is stored in a hybrid row-columnar format
- Analytical queries only scan the columns they need (massive speedup)
- Typical compression ratios: 90%+ for time-series data
- Compression happens transparently - no changes to your queries

### Direct to Columnstore
When you use `SET timescaledb.enable_direct_compress_copy = on`:
- Data loads directly into compressed columnstore format
- Bypasses the rowstore entirely
- Instant analytical performance - no waiting for background compression
- Perfect for bulk data loads and migrations

### Segmentation
The `tsdb.segmentby='pickup_boroname'` setting:
- Groups data by pickup borough within each chunk (6 unique values: Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR)
- Improves compression ratios (similar data together)
- Speeds up queries that filter by pickup_boroname
- Better cardinality than vendor_id (6 values vs 2) for optimal compression
- Automatically optimized without manual tuning

### time_bucket() Function
TimescaleDB's `time_bucket()` is like PostgreSQL's `date_trunc()` but more powerful:
- Works with any interval: `5 minutes`, `1 hour`, `1 day`, etc.
- Optimized for time-series queries
- Integrates seamlessly with continuous aggregates
- Essential for time-series analytics

## Sample Queries Explained

See [`nyc-taxi-queries.sql`](nyc-taxi-queries.sql) for the complete set of queries. Each query demonstrates:

1. **Total trips and revenue** - Simple aggregations across all data
2. **Breakdown by vendor** - Segmentation analysis by taxi vendor
3. **Hourly patterns** - Using `time_bucket()` for time-based aggregation
4. **Payment type analysis** - Analyzing payment methods
5. **Daily statistics** - Multi-dimensional aggregation (time + borough)
6. **Distance categories** - CASE statement with aggregations

## Schema Design Choices

### Why these settings?

**partition_column='pickup_datetime'**
- Time is the natural partition key for time-series data
- Enables automatic chunk pruning for time-range queries
- Default chunk interval (7 days) works well for taxi data

**segmentby='pickup_boroname'**
- Optimal cardinality with 6 borough values (Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR)
- Frequently used in WHERE clauses and GROUP BY for location-based analytics
- Improves compression by grouping geographically similar trips
- Better than vendor_id (only 2 values) for compression efficiency

**orderby='pickup_datetime DESC'**
- Most queries want recent data first
- Optimizes for "latest trips" queries
- Improves query performance for time-range scans

## Continuous Aggregates (Advanced)

For real-time dashboards, you can create continuous aggregates that automatically update:

```sql
-- Create a continuous aggregate for hourly statistics by borough
CREATE MATERIALIZED VIEW trips_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', pickup_datetime) AS hour,
    pickup_boroname,
    COUNT(*) as trip_count,
    AVG(fare_amount) as avg_fare,
    SUM(fare_amount) as total_revenue,
    AVG(trip_distance) as avg_distance
FROM trips
GROUP BY hour, pickup_boroname;

-- Add a refresh policy to keep it updated
SELECT add_continuous_aggregate_policy('trips_hourly',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
```

Now you can query `trips_hourly` for instant results on pre-aggregated data.


## Troubleshooting

### Data didn't load
- Check the CSV file path is correct
- Ensure the CSV header matches the schema columns
- Try loading a few rows first to test: `LIMIT 10` in your data file

### Direct to columnstore not working
- Verify TimescaleDB version 2.24+: `SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';`
- Ensure you ran `SET timescaledb.enable_direct_compress_copy = on;` in the same session
- Check for error messages in the output

### Queries seem slow
- Verify columnstore is enabled: `SELECT * FROM timescaledb_information.hypertables WHERE hypertable_name = 'trips';`
- Check if data is compressed: `SELECT * FROM timescaledb_information.chunks WHERE hypertable_name = 'trips';`
- Ensure you're querying with time ranges (enables chunk exclusion)

### Out of memory during load
- Reduce batch size in COPY command
- Increase Docker memory allocation
- Consider loading data in smaller time-range batches

## Use Cases

This NYC Taxi example demonstrates patterns applicable to:

- **Ride-sharing platforms** - Track trips, drivers, pricing
- **Fleet management** - Vehicle tracking, route optimization
- **Delivery services** - Order tracking, delivery times, driver analytics
- **Public transportation** - Route analysis, passenger counts, schedule optimization
- **Urban planning** - Traffic patterns, popular routes, demand forecasting
- **Logistics** - Shipment tracking, route efficiency, cost analysis

## Clean Up

When you're done experimenting:

#### If you used the one-line install:

```bash
# Stop the container
docker stop timescaledb-ha-pg18-quickstart

# Remove the container
docker rm timescaledb-ha-pg18-quickstart

# Remove the persistent data volume
docker volume rm timescaledb_data

# (Optional) Remove the Docker image
docker rmi timescale/timescaledb-ha:pg18
```

#### If you used the manual Docker command:

```bash
# Stop the container
docker stop timescaledb

# Remove the container
docker rm timescaledb

# (Optional) Remove the Docker image
docker rmi timescale/timescaledb-ha:pg18
```

**Note:** If you created a named volume with the manual Docker command, you can remove it with `docker volume rm <volume_name>`.

## Contributing

Found an issue or want to improve this example? Contributions welcome! Open an issue or PR on [GitHub](https://github.com/timescale/timescaledb).

---

**Questions?** Check out [Timescale Community Forums](https://www.timescale.com/forum) or [TimescaleDB Documentation](https://docs.timescale.com).
