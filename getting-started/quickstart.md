# TimescaleDB Quick Start Guide

Get started with TimescaleDB in under 20 minutes. This guide will help you run TimescaleDB locally, create your first hypertable with columnstore enabled, write data to the columnstore, and see instant analytical query performance.

## What You'll Learn

- How to run TimescaleDB with a single Docker command
- How to create a hypertable with columnstore enabled
- How to insert data directly to the columnstore 
- How to query time-series data

## Prerequisites

- Docker installed on your machine
- 8GB RAM recommended
- `psql` client (included with PostgreSQL) or any PostgreSQL client like [pgAdmin](https://www.pgadmin.org/download/)

## Step 1: Start TimescaleDB (5 minutes)

Run TimescaleDB using Docker with a single command:

```bash
docker run -d --name timescaledb \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD=password \
    timescale/timescaledb-ha:pg17
```

This command:
- Downloads and starts TimescaleDB (if not already downloaded)
- Exposes PostgreSQL on port 5432
- Sets the password to `password` (change this for production!)
- Automatically tunes settings for your environment

Wait about 10-15 seconds for TimescaleDB to initialize.

## Step 2: Connect to TimescaleDB (1 minute)

Connect using `psql`:

```bash
psql -h localhost -p 5432 -U postgres
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

**Prefer a GUI?** If you'd rather use a graphical tool instead of the command line, you can download [pgAdmin](https://www.pgadmin.org/download/) and connect to TimescaleDB using the same connection details (host: `localhost`, port: `5432`, user: `postgres`, password: `password`).

## Step 3: Create Your First Hypertable (1 minutes)

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
    --,tsdb.segmentby='sensor_id' TODO: I want to be able to remove these and be smart about the default settings
	--,tsdb.orderby   = 'time DESC'
);
```

`tsdb.hypertable` - Converts this into a TimescaleDB hypertable

## Step 4: Insert Sample Data (5 minutes)

Let's add some sample sensor readings:

```sql
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

-- re-sort data (TODO: Understand if we can optimize this from the start)
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

## Step 5: Run Your First Analytical Queries(5 minutes)

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


## Next Steps

Now that you've got the basics, explore more:

### Try Complete Examples

Check out our complete examples with real-world datasets:

- **[IoT Sensors](examples/iot-sensors/)** - Device monitoring and analytics
- **[NYC Taxi Data](examples/nyc-taxi/)** - Transportation and location analytics
- **[Financial Ticks](examples/financial-ticks/)** - Market data and trading analytics
- **[Events with UUIDv7](examples/events-uuidv7/)** - Application event logging
- **[Cryptocurrency](examples/crypto/)** - Crypto market data analysis

### Bring Your Own Data

Ready to try TimescaleDB with your data? See our guide:
- [Your Own Data Guide](your-own-data/)

### Learn More

- [TimescaleDB Documentation](https://docs.timescale.com)
- [Time-series Best Practices](https://docs.timescale.com/use-timescale/latest/schema-management/)
- [Continuous Aggregates](https://docs.timescale.com/use-timescale/latest/continuous-aggregates/)
- [Data Retention Policies](https://docs.timescale.com/use-timescale/latest/data-retention/)

## Troubleshooting

### Docker container won't start

```bash
# Check if container is running
docker ps -a

# View container logs
docker logs timescaledb

# Stop and remove existing container
docker stop timescaledb && docker rm timescaledb

# Start fresh
docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb-ha:pg17
```

### Can't connect with psql

- Verify Docker container is running: `docker ps`
- Check port 5432 isn't already in use: `lsof -i :5432`
- Try using explicit host: `psql -h 127.0.0.1 -p 5432 -U postgres`

### TimescaleDB extension not found

The `timescale/timescaledb-ha:pg17` image has TimescaleDB pre-installed and pre-loaded. If you see errors, ensure you're using the correct image.

### Query performance seems slow

For this small sample dataset, queries should be very fast (milliseconds). If you load larger datasets:
- Ensure columnstore is enabled: `tsdb.enable_columnstore=true`
- Check if data is compressed: `SELECT * FROM timescaledb_information.chunks;`
- For instant performance with bulk loads, use direct to columnstore (covered in the examples)

## Clean Up

When you're done experimenting:

```bash
# Stop the container
docker stop timescaledb

# Remove the container
docker rm timescaledb

# (Optional) Remove the Docker image
docker rmi timescale/timescaledb-ha:pg17
```

---

**Ready for more?** Check out our [complete examples](examples/) with real-world datasets and production-ready patterns.
