# TimescaleDB Quick Start Guide

Get started with TimescaleDB in under 10 minutes. This guide will help you run TimescaleDB locally, create your first hypertable with columnstore enabled, write data to the columnstore, and see instant analytical query performance.

## What You'll Learn

- How to run TimescaleDB with a one-line install or Docker command
- How to create a hypertable with columnstore enabled
- How to insert data directly to the columnstore 
- How to execute analytical queries

## Prerequisites

- Docker installed on your machine
- 8GB RAM recommended
- `psql` client (included with PostgreSQL) or any PostgreSQL client like [pgAdmin](https://www.pgadmin.org/download/)

## Step 1: Start TimescaleDB

You have two options to start TimescaleDB:

### Option 1: One-line install (Recommended)

The easiest way to get started:

> **Important:** This script is intended for local development and testing only. Do **not** use it for production deployments. For production-ready installation options, see the [TimescaleDB installation guide](https://docs.timescale.com/self-hosted/latest/install/).

**Linux/Mac:**

```sh
curl -sL https://tsdb.co/start-local | sh
```

**Windows (PowerShell):**

```powershell
iwr -useb https://tsdb.co/start-local-windows | iex
```

This command:
- Downloads and starts TimescaleDB (if not already downloaded)
- Exposes PostgreSQL on port **6543** (a non-standard port to avoid conflicts with other PostgreSQL instances on port 5432)
- Automatically tunes settings for your environment using timescaledb-tune
- Sets up a persistent data volume

### Option 2: Manual Docker command

Alternatively, you can run TimescaleDB directly with Docker:

```bash
docker run -d --name timescaledb \
    -p 6543:5432 \
    -e POSTGRES_PASSWORD=password \
    timescale/timescaledb-ha:pg18
```

**Note:** We use port **6543** (mapped to container port 5432) to avoid conflicts if you have other PostgreSQL instances running on the standard port 5432.

Wait about 1-2 minutes for TimescaleDB to download & initialize.

## Step 2: Connect to TimescaleDB

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

## Step 3: Create Your First Hypertable

Let's create a hypertable for IoT sensor data with columnstore enabled:

```sql
-- Create a hypertable with automatic columnstore
CREATE TABLE sensor_data (
    time TIMESTAMPTZ NOT NULL,
    sensor_id TEXT NOT NULL,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    pressure DOUBLE PRECISION
) WITH (
    tsdb.hypertable
);
-- create index
CREATE INDEX idx_sensor_id_time ON sensor_data(sensor_id, time DESC);
```

`tsdb.hypertable` - Converts this into a TimescaleDB hypertable

See more:

- [About hypertables](https://docs.tigerdata.com/use-timescale/latest/hypertables/)
- [API reference](https://docs.tigerdata.com/api/latest/hypertable/)
- [About columnstore](https://docs.tigerdata.com/use-timescale/latest/compression/about-compression/)
- [Enable columnstore manually](https://docs.tigerdata.com/use-timescale/latest/compression/manual-compression/)
- [API reference](https://docs.tigerdata.com/api/latest/compression/)

## Step 4: Insert Sample Data

Let's add some sample sensor readings:


```sql
-- Enable timing to see time to execute queries
\timing on

-- Insert sample data for multiple sensors
-- SET timescaledb.enable_direct_compress_insert = on to insert data directly to the columnstore (columnnar format for performance)
SET timescaledb.enable_direct_compress_insert = on;
INSERT INTO sensor_data (time, sensor_id, temperature, humidity, pressure)
SELECT
    time,
    'sensor_' || ((random() * 9)::int + 1),
    20 + (random() * 15),
    40 + (random() * 30),
    1000 + (random() * 50)
FROM generate_series(
    NOW() - INTERVAL '90 days',
    NOW(),
    INTERVAL '1 seconds'
) AS time;

-- Once data is inserted into the columnstore we use recompress to optimize the order and structure 
-- of the each chunk to ensure optimal performance during querying
DO $$
DECLARE ch TEXT;
BEGIN
    FOR ch IN SELECT show_chunks('sensor_data') LOOP
        CALL convert_to_columnstore(ch, recompress := true);
    END LOOP;
END $$;
```

This generates ~7,776,001 readings across 10 sensors over the past 90 days.

Verify the data was inserted:

```sql
SELECT COUNT(*) FROM sensor_data;
```

## Step 5: Run Your First Analytical Queries

Now let's run some analytical queries that showcase TimescaleDB's performance:

```sql
-- Enable query timing to see performance
\timing on

-- Query 1: Average readings per sensor over the last 7 days
SELECT
    sensor_id,
    COUNT(*) as readings,
    ROUND(AVG(temperature)::numeric, 2) as avg_temp,
    ROUND(AVG(humidity)::numeric, 2) as avg_humidity,
    ROUND(AVG(pressure)::numeric, 2) as avg_pressure
FROM sensor_data
WHERE time > NOW() - INTERVAL '7 days'
GROUP BY sensor_id
ORDER BY sensor_id;

-- Query 2: Hourly averages using time_bucket (TimescaleDB's superpower)
SELECT
    time_bucket('1 hour', time) AS hour,
    sensor_id,
    ROUND(AVG(temperature)::numeric, 2) as avg_temp,
    ROUND(AVG(humidity)::numeric, 2) as avg_humidity
FROM sensor_data
WHERE time > NOW() - INTERVAL '24 hours'
GROUP BY hour, sensor_id
ORDER BY hour DESC, sensor_id
LIMIT 20;

-- Query 3: Daily statistics across all sensors
SELECT
    time_bucket('1 day', time) AS day,
    COUNT(*) as total_readings,
    ROUND(AVG(temperature)::numeric, 2) as avg_temp,
    ROUND(MIN(temperature)::numeric, 2) as min_temp,
    ROUND(MAX(temperature)::numeric, 2) as max_temp
FROM sensor_data
GROUP BY day
ORDER BY day DESC
LIMIT 10;

-- Query 4: Latest reading for each sensor
-- Highlights the value of Skipscan executing in under 100ms without skipscan it takes over 5sec
SELECT DISTINCT ON (sensor_id)
    sensor_id,
    time,
    ROUND(temperature::numeric, 2) as temperature,
    ROUND(humidity::numeric, 2) as humidity,
    ROUND(pressure::numeric, 2) as pressure
FROM sensor_data
ORDER BY sensor_id, time DESC;
```

Notice how fast these analytical queries run, even with aggregations across millions of rows. This is the power of TimescaleDB's columnstore.

## What's Happening Behind the Scenes?

TimescaleDB automatically:
- **Partitions your data** into time-based chunks for efficient querying
- **Write directly to columnstore** using columnar storage (90%+ compression typical) and faster vectorized queries
- **Optimizes queries** by only scanning relevant time ranges and columns
- **Enables time_bucket()** - a powerful function for time-series aggregation

See more:

- [Query data](https://docs.tigerdata.com/use-timescale/latest/query-data/)
- [Write data](https://docs.tigerdata.com/use-timescale/latest/write-data/)
- [About time buckets](https://docs.tigerdata.com/use-timescale/latest/time-buckets/about-time-buckets/)
- [API reference](https://docs.tigerdata.com/api/latest/hyperfunctions/time_bucket/)
- [All TimescaleDB features](https://docs.tigerdata.com/use-timescale/latest/)

## Next Steps

Now that you've got the basics, explore more:

### Try Complete Examples

Check out our complete examples with real-world datasets:

- **[IoT Sensors](examples/iot-sensors/)** - Device monitoring and analytics
- **[NYC Taxi Data](examples/nyc-taxi/)** - Transportation and location analytics
- **[Financial Ticks](examples/financial-ticks/)** - Market data and trading analytics
- **[Events with UUIDv7](examples/events-uuidv7/)** - Application event logging
- **[Cryptocurrency](examples/crypto/)** - Crypto market data analysis

### Learn More

- [TimescaleDB Documentation](https://docs.timescale.com)
- [Time-series Best Practices](https://docs.timescale.com/use-timescale/latest/schema-management/)
- [Continuous Aggregates](https://docs.timescale.com/use-timescale/latest/continuous-aggregates/)

## Create Continuous Aggregates

Continuous aggregates make real-time analytics run faster on very large datasets. They continuously and incrementally refresh a query in the background, so that when you run such query, only the data that has changed needs to be computed, not the entire dataset. This is what makes them different from regular PostgreSQL [materialized views](https://www.postgresql.org/docs/current/rules-materializedviews.html), which cannot be incrementally materialized and have to be rebuilt from scratch every time you want to refresh them.

Let's create a continuous aggregate for hourly sensor statistics:

### Step 1: Create the Continuous Aggregate

```sql
CREATE MATERIALIZED VIEW sensor_data_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS hour,
    sensor_id,
    AVG(temperature) AS avg_temp,
    AVG(humidity) AS avg_humidity,
    AVG(pressure) AS avg_pressure,
    MIN(temperature) AS min_temp,
    MAX(temperature) AS max_temp,
    COUNT(*) AS reading_count
FROM sensor_data
GROUP BY hour, sensor_id;
```

This creates a materialized view that pre-aggregates your sensor data into hourly buckets. The view is automatically populated with existing data.

### Step 2: Add a Refresh Policy

To keep the continuous aggregate up-to-date as new data arrives, add a refresh policy:

```sql
SELECT add_continuous_aggregate_policy(
    'sensor_data_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);
```

This policy:
- Refreshes the continuous aggregate every hour
- Processes data from 3 hours ago up to 1 hour ago (leaving the most recent hour for real-time queries)
- Only processes new or changed data incrementally

### Step 3: Query the Continuous Aggregate

Now you can query the pre-aggregated data for much faster results:

```sql
-- Get hourly averages for the last 24 hours
SELECT
    hour,
    sensor_id,
    ROUND(avg_temp::numeric, 2) AS avg_temp,
    ROUND(avg_humidity::numeric, 2) AS avg_humidity,
    reading_count
FROM sensor_data_hourly
WHERE hour > NOW() - INTERVAL '24 hours'
ORDER BY hour DESC, sensor_id
LIMIT 50;
```

### Benefits of Continuous Aggregates

- **Faster queries**: Pre-aggregated data means queries run in milliseconds instead of seconds
- **Incremental refresh**: Only new/changed data is processed, not the entire dataset
- **Automatic updates**: The refresh policy keeps your aggregates current without manual intervention
- **Real-time option**: You can enable real-time aggregation to combine materialized and raw data

### Try It Yourself

Compare the performance difference:

```sql
-- Query the raw hypertable (slower on large datasets)
\timing on
SELECT
    time_bucket('1 hour', time) AS hour,
    AVG(temperature) AS avg_temp
FROM sensor_data
WHERE time > NOW() - INTERVAL '60 days'
GROUP BY hour
ORDER BY hour DESC
LIMIT 24;

-- Query the continuous aggregate (much faster)
SELECT
    hour,
    avg_temp
FROM sensor_data_hourly
WHERE hour > NOW() - INTERVAL '60 days'
ORDER BY hour DESC
LIMIT 24;
```

Notice how the continuous aggregate query is significantly faster, especially as your dataset grows!

See more:

- [About continuous aggregates](https://docs.tigerdata.com/use-timescale/latest/continuous-aggregates/)
- [API reference](https://docs.tigerdata.com/api/latest/continuous-aggregates/create_materialized_view/)

## Troubleshooting

### Docker container won't start

```bash
# Check if container is running
docker ps -a

# View container logs (use the appropriate container name)
# For one-line install:
docker logs timescaledb-ha-pg18-quickstart
# For manual Docker command:
docker logs timescaledb

# Stop and remove existing container
# For one-line install:
docker stop timescaledb-ha-pg18-quickstart && docker rm timescaledb-ha-pg18-quickstart
# For manual Docker command:
docker stop timescaledb && docker rm timescaledb

# Start fresh
# Option 1: Use the one-line install
curl -sL https://tsdb.co/start-local | sh
# Option 2: Use manual Docker command
docker run -d --name timescaledb -p 6543:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb-ha:pg18
```

### Can't connect with psql

- Verify Docker container is running: `docker ps`
- Check port 6543 isn't already in use: `lsof -i :6543`
- Try using explicit host: `psql -h 127.0.0.1 -p 6543 -U postgres`

### TimescaleDB extension not found

The `timescale/timescaledb-ha:pg18` image has TimescaleDB pre-installed and pre-loaded. If you see errors, ensure you're using the correct image.

## Clean Up

When you're done experimenting:

### If you used the one-line install:

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

### If you used the manual Docker command:

```bash
# Stop the container
docker stop timescaledb

# Remove the container
docker rm timescaledb

# (Optional) Remove the Docker image
docker rmi timescale/timescaledb-ha:pg18
```

**Note:** If you created a named volume with the manual Docker command, you can remove it with `docker volume rm <volume_name>`.

---

**Ready for more?** Check out our [complete examples](examples/) with real-world datasets and production-ready patterns.
