# NYC Taxi Data Example

Get started with TimescaleDB using New York City taxi trip data. This example demonstrates how to handle high-volume transportation data with location-based analytics and time-series aggregations.

## What You'll Learn

- How to model high-cardinality transportation data (locations, cab types)
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

```bash
docker run -d --name timescaledb \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD=password \
    timescale/timescaledb-ha:pg17
```

Wait 10-15 seconds for the container to start, then connect:

```bash
psql -h localhost -p 5432 -U postgres
# Password: password
```

### Step 2: Create the Schema

Run the schema definition to create the optimized hypertable:

```bash
# From your terminal (outside psql)
psql -h localhost -p 5432 -U postgres < nyc-taxi-schema.sql
```

Or copy-paste the contents of [`nyc-taxi-schema.sql`](nyc-taxi-schema.sql) directly into your `psql` session.

This creates a `trips` table with:
- Automatic time-based partitioning on `pickup_datetime`
- Columnstore enabled for fast analytical queries
- Segmentation by `cab_type` for optimal compression
- Indexes for common query patterns

### Step 3: Load Sample Data

We provide two approaches for loading data. Choose based on your needs:

#### Option A: Direct to Columnstore (Recommended - Instant Performance)

This approach writes data directly to the columnstore, bypassing the rowstore entirely. You get instant analytical performance.

**From psql:**

```sql
-- Enable direct to columnstore for this session
SET timescaledb.enable_direct_compress_copy = on;

-- Load data directly into columnstore
\COPY trips FROM 'nyc-taxi-sample.csv' WITH (FORMAT csv, HEADER true);

-- Verify data loaded
SELECT COUNT(*) FROM trips;
```

**From command line:**

```bash
psql -h localhost -p 5432 -U postgres \
  -v ON_ERROR_STOP=1 \
  -c "SET timescaledb.enable_direct_compress_copy = on;
      COPY trips FROM STDIN WITH (FORMAT csv, HEADER true);" \
  < nyc-taxi-sample.csv
```

#### Option B: Standard COPY (Fallback)

This approach loads data into the rowstore first. Data will be compressed by a background policy (takes longer).

```sql
-- Standard COPY without direct to columnstore
\COPY trips FROM 'nyc-taxi-sample.csv' WITH (FORMAT csv, HEADER true);

-- Verify data loaded
SELECT COUNT(*) FROM trips;

-- Optional: Manually compress data to columnstore instead of waiting for policy
SELECT compress_chunk(chunk) FROM show_chunks('trips');
```

### Step 4: Run Sample Queries

Now let's explore the data with some analytical queries:

```bash
# Run all sample queries
psql -h localhost -p 5432 -U postgres < nyc-taxi-queries.sql
```

Or run queries individually in your `psql` session. Here are some highlights:

**Overall statistics:**
```sql
\timing on

SELECT
    COUNT(*) as total_trips,
    ROUND(SUM(fare_amount)::numeric, 2) as total_revenue,
    ROUND(AVG(fare_amount)::numeric, 2) as avg_fare,
    ROUND(AVG(trip_distance)::numeric, 2) as avg_distance
FROM trips;
```

**Hourly patterns using time_bucket:**
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

**Top revenue locations:**
```sql
SELECT
    pickup_location_id,
    COUNT(*) as trip_count,
    ROUND(SUM(fare_amount)::numeric, 2) as total_revenue,
    ROUND(AVG(trip_distance)::numeric, 2) as avg_distance,
    ROUND(AVG(tip_amount)::numeric, 2) as avg_tip
FROM trips
GROUP BY pickup_location_id
ORDER BY total_revenue DESC
LIMIT 10;
```

Notice how fast these analytical queries execute, even with aggregations across thousands of rows. This is the power of TimescaleDB's columnstore.

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
The `tsdb.segmentby='cab_type'` setting:
- Groups data by cab type within each chunk
- Improves compression ratios (similar data together)
- Speeds up queries that filter by cab_type
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
2. **Breakdown by cab type** - Segmentation analysis
3. **Hourly patterns** - Using `time_bucket()` for time-based aggregation
4. **Top pickup locations** - High-cardinality group by analysis
5. **Daily statistics** - Multi-dimensional aggregation (time + cab type)
6. **Distance categories** - CASE statement with aggregations

## Schema Design Choices

### Why these settings?

**partition_column='pickup_datetime'**
- Time is the natural partition key for time-series data
- Enables automatic chunk pruning for time-range queries
- Default chunk interval (7 days) works well for taxi data

**segmentby='cab_type'**
- Low to medium cardinality (yellow, green, uber, etc.)
- Frequently used in WHERE clauses and GROUP BY
- Improves compression by grouping similar trips

**orderby='pickup_datetime DESC'**
- Most queries want recent data first
- Optimizes for "latest trips" queries
- Improves query performance for time-range scans

### Index Strategy

We created two indexes:
1. `(pickup_location_id, pickup_datetime DESC)` - For location-based queries
2. `(cab_type, pickup_datetime DESC)` - For cab-type filtered queries

These cover the most common query patterns while keeping index overhead minimal.

## Common Query Patterns

### Time-range queries
```sql
-- Last 24 hours of trips
SELECT * FROM trips
WHERE pickup_datetime > NOW() - INTERVAL '24 hours';
```

### Aggregation with time_bucket
```sql
-- 15-minute intervals
SELECT
    time_bucket('15 minutes', pickup_datetime) AS bucket,
    COUNT(*) as trips,
    AVG(fare_amount) as avg_fare
FROM trips
WHERE pickup_datetime > NOW() - INTERVAL '7 days'
GROUP BY bucket
ORDER BY bucket DESC;
```

### Location-based analysis
```sql
-- Trips from a specific pickup location
SELECT
    COUNT(*) as trips,
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance
FROM trips
WHERE pickup_location_id = 161
    AND pickup_datetime > NOW() - INTERVAL '7 days';
```

## Continuous Aggregates (Advanced)

For real-time dashboards, you can create continuous aggregates that automatically update:

```sql
-- Create a continuous aggregate for hourly statistics
CREATE MATERIALIZED VIEW trips_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', pickup_datetime) AS hour,
    cab_type,
    COUNT(*) as trip_count,
    AVG(fare_amount) as avg_fare,
    SUM(fare_amount) as total_revenue,
    AVG(trip_distance) as avg_distance
FROM trips
GROUP BY hour, cab_type;

-- Add a refresh policy to keep it updated
SELECT add_continuous_aggregate_policy('trips_hourly',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
```

Now you can query `trips_hourly` for instant results on pre-aggregated data.

## Data Retention Policies (Advanced)

For production systems, you can automatically drop old data:

```sql
-- Keep only the last 90 days of raw data
SELECT add_retention_policy('trips', INTERVAL '90 days');
```

Continuous aggregates can retain data longer than raw data, giving you historical trends without storing all the details.

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
- Ensure you're querying with time ranges (enables chunk pruning)

### Out of memory during load
- Reduce batch size in COPY command
- Increase Docker memory allocation
- Consider loading data in smaller time-range batches

## Next Steps

### Explore More Examples
- [IoT Sensors](../iot-sensors/) - Device monitoring patterns
- [Financial Ticks](../financial-ticks/) - Market data analytics
- [Events with UUIDv7](../events-uuidv7/) - Application event logging
- [Cryptocurrency](../crypto/) - Crypto market analysis

### Bring Your Own Data
Ready to try TimescaleDB with your data? See our guide:
- [Your Own Data Guide](../../your-own-data/)

### Learn Advanced Features
- [Continuous Aggregates](https://docs.timescale.com/use-timescale/latest/continuous-aggregates/)
- [Data Retention Policies](https://docs.timescale.com/use-timescale/latest/data-retention/)
- [Compression Settings](https://docs.timescale.com/use-timescale/latest/compression/)
- [TimescaleDB Best Practices](https://docs.timescale.com/use-timescale/latest/schema-management/)

### Production Deployment
- [Timescale Cloud](https://www.timescale.com/cloud) - Managed TimescaleDB hosting
- [Self-hosting Guide](https://docs.timescale.com/self-hosted/latest/)
- [Performance Tuning](https://docs.timescale.com/self-hosted/latest/configuration/)

## Use Cases

This NYC Taxi example demonstrates patterns applicable to:

- **Ride-sharing platforms** - Track trips, drivers, pricing
- **Fleet management** - Vehicle tracking, route optimization
- **Delivery services** - Order tracking, delivery times, driver analytics
- **Public transportation** - Route analysis, passenger counts, schedule optimization
- **Urban planning** - Traffic patterns, popular routes, demand forecasting
- **Logistics** - Shipment tracking, route efficiency, cost analysis

## Files in This Example

- [`README.md`](README.md) - This file
- [`nyc-taxi-schema.sql`](nyc-taxi-schema.sql) - Table schema and indexes
- [`nyc-taxi-sample.csv`](nyc-taxi-sample.csv) - Sample taxi trip data (~1000 rows)
- [`nyc-taxi-queries.sql`](nyc-taxi-queries.sql) - Sample analytical queries

## Contributing

Found an issue or want to improve this example? Contributions welcome! Open an issue or PR on [GitHub](https://github.com/timescale/timescaledb).

---

**Questions?** Check out [Timescale Community Forums](https://www.timescale.com/forum) or [TimescaleDB Documentation](https://docs.timescale.com).
