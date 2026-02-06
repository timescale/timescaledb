<div align=center>
<picture align=center>
    <source  srcset="https://assets.timescale.com/timescale-web/brand/show/horizontal-black.svg">
    <img alt="Tiger Data logo" >
</picture>
</div>

<div align=center>

<h3>TimescaleDB is a PostgreSQL extension for high-performance real-time analytics on time-series and event data</h3>

[![Docs](https://img.shields.io/badge/Read_the_docs-black?style=for-the-badge&logo=readthedocs&logoColor=white)](https://docs.tigerdata.com/)
[![SLACK](https://img.shields.io/badge/Ask_the_community-black?style=for-the-badge&logo=slack&logoColor=white)](https://timescaledb.slack.com/archives/C4GT3N90X)
[![Try TimescaleDB for free](https://img.shields.io/badge/Try_Tiger_Cloud_for_free-black?style=for-the-badge&logo=timescale&logoColor=white)](https://console.cloud.timescale.com/signup)

</div>

## Quick Start with TimescaleDB

Get started with TimescaleDB in under 10 minutes. This guide will help you run TimescaleDB locally, create your first hypertable with columnstore enabled, write data to the columnstore, and see instant analytical query performance.

### What You'll Learn

- How to run TimescaleDB with a one-line install or Docker command
- How to create a hypertable with columnstore enabled
- How to insert data directly to the columnstore 
- How to execute analytical queries

### Prerequisites

- Docker installed on your machine
- 8GB RAM recommended
- `psql` client (included with PostgreSQL) or any PostgreSQL client like [pgAdmin](https://www.pgadmin.org/download/)

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

### Step 3: Create Your First Hypertable

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

### Step 4: Insert Sample Data

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

-- Once data is inserted into the columnstore we optimize the order and structure 
-- this compacts and orders the data in the chunks for optimal query performance and compression
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

### Step 5: Run Your First Analytical Queries

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

-- Query 2: Hourly averages using time_bucket 
-- Time buckets enable you to aggregate data in hypertables by time interval and calculate summary values.
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

### What's Happening Behind the Scenes?

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

### Next Steps

Now that you've got the basics, explore more:

### Create Continuous Aggregates

Continuous aggregates make real-time analytics run faster on very large datasets. They continuously and incrementally refresh a query in the background, so that when you run such query, only the data that has changed needs to be computed, not the entire dataset. This is what makes them different from regular PostgreSQL [materialized views](https://www.postgresql.org/docs/current/rules-materializedviews.html), which cannot be incrementally materialized and have to be rebuilt from scratch every time you want to refresh them.

Let's create a continuous aggregate for hourly sensor statistics:

#### Step 1: Create the Continuous Aggregate

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

#### Step 2: Add a Refresh Policy

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

#### Step 3: Query the Continuous Aggregate

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

#### Benefits of Continuous Aggregates

- **Faster queries**: Pre-aggregated data means queries run in milliseconds instead of seconds
- **Incremental refresh**: Only new/changed data is processed, not the entire dataset
- **Automatic updates**: The refresh policy keeps your aggregates current without manual intervention
- **Real-time option**: You can enable real-time aggregation to combine materialized and raw data

#### Try It Yourself

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
- [TimescaleDB Documentation](https://docs.timescale.com)
- [Time-series Best Practices](https://docs.timescale.com/use-timescale/latest/schema-management/)
- [Continuous Aggregates](https://docs.timescale.com/use-timescale/latest/continuous-aggregates/)

## Examples

Learn TimescaleDB with complete, standalone examples using real-world datasets. Each example includes sample data and analytical queries.

- **[NYC Taxi Data](docs/getting-started/nyc-taxi/)** - Transportation and location-based analytics
- **[Financial Market Data](docs/getting-started/financial-ticks/)** - Trading and market data analysis
- **[Application Events](docs/getting-started/events-uuidv7/)** - Event logging with UUIDv7

Or try some of our workshops
- **[AI Workshop: EV Charging Station Analysis](https://github.com/timescale/TigerData-Workshops/tree/main/AI-Workshop)** - Integrate PostgreSQL with AI capabilities for managing and analyzing EV charging station data
- **[Time-Series Workshop: Financial Data Analysis](https://github.com/timescale/TigerData-Workshops/tree/main/TimeSeries-Workshop-Finance/)** - Work with cryptocurrency tick data, create candlestick charts

## Want TimescaleDB hosted and managed for you? Try Tiger Cloud

[Tiger Cloud](https://docs.tigerdata.com/getting-started/latest/) is the modern PostgreSQL data platform for all your applications. It enhances PostgreSQL to handle time series, events, real-time analytics, and vector search—all in a single database alongside transactional workloads. You get one system that handles live data ingestion, late and out-of-order updates, and low latency queries, with the performance, reliability, and scalability your app needs. Ideal for IoT, crypto, finance, SaaS, and a myriad other domains, Tiger Cloud allows you to build data-heavy, mission-critical apps while retaining the familiarity and reliability of PostgreSQL. See [our whitepaper](https://docs.tigerdata.com/about/latest/whitepaper/) for a deep dive into Tiger Cloud's architecture and how it meets the needs of even the most demanding applications.

A Tiger Cloud service is a single optimized 100% PostgreSQL database instance that you use as is, or extend with capabilities specific to your business needs. The available capabilities are:

- **Time-series and analytics**: PostgreSQL with TimescaleDB. The PostgreSQL you know and love, supercharged with functionality for storing and querying time-series data at scale for real-time analytics and other use cases. Get faster time-based queries with hypertables, continuous aggregates, and columnar storage. Save on storage with native compression, data retention policies, and bottomless data tiering to Amazon S3.
- **AI and vector**: PostgreSQL with vector extensions. Use PostgreSQL as a vector database with purpose built extensions for building AI applications from start to scale. Get fast and accurate similarity search with the pgvector and pgvectorscale extensions. Create vector embeddings and perform LLM reasoning on your data with the pgai extension.
- **PostgreSQL**: the trusted industry-standard RDBMS. Ideal for applications requiring strong data consistency, complex relationships, and advanced querying capabilities. Get ACID compliance, extensive SQL support, JSON handling, and extensibility through custom functions, data types, and extensions.
All services include all the cloud tooling you'd expect for production use: [automatic backups](https://docs.tigerdata.com/use-timescale/latest/backup-restore/backup-restore-cloud/), [high availability](https://docs.tigerdata.com/use-timescale/latest/ha-replicas/), [read replicas](https://docs.tigerdata.com/use-timescale/latest/ha-replicas/read-scaling/), [data forking](https://docs.tigerdata.com/use-timescale/latest/services/service-management/#fork-a-service), [connection pooling](https://docs.tigerdata.com/use-timescale/latest/services/connection-pooling/), [tiered storage](https://docs.tigerdata.com/use-timescale/latest/data-tiering/), [usage-based storage](https://docs.tigerdata.com/about/latest/pricing-and-account-management/), and much more.

## Check build status

|Linux/macOS|Linux i386|Windows|Coverity|Code Coverage|OpenSSF|
|:---:|:---:|:---:|:---:|:---:|:---:|
|[![Build Status Linux/macOS](https://github.com/timescale/timescaledb/actions/workflows/linux-build-and-test.yaml/badge.svg?branch=main&event=schedule)](https://github.com/timescale/timescaledb/actions/workflows/linux-build-and-test.yaml?query=workflow%3ARegression+branch%3Amain+event%3Aschedule)|[![Build Status Linux i386](https://github.com/timescale/timescaledb/actions/workflows/linux-32bit-build-and-test.yaml/badge.svg?branch=main&event=schedule)](https://github.com/timescale/timescaledb/actions/workflows/linux-32bit-build-and-test.yaml?query=workflow%3ARegression+branch%3Amain+event%3Aschedule)|[![Windows build status](https://github.com/timescale/timescaledb/actions/workflows/windows-build-and-test.yaml/badge.svg?branch=main&event=schedule)](https://github.com/timescale/timescaledb/actions/workflows/windows-build-and-test.yaml?query=workflow%3ARegression+branch%3Amain+event%3Aschedule)|[![Coverity Scan Build Status](https://scan.coverity.com/projects/timescale-timescaledb/badge.svg)](https://scan.coverity.com/projects/timescale-timescaledb)|[![Code Coverage](https://codecov.io/gh/timescale/timescaledb/branch/main/graphs/badge.svg?branch=main)](https://codecov.io/gh/timescale/timescaledb)|[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/8012/badge)](https://www.bestpractices.dev/projects/8012)|

## Get involved

We welcome contributions to TimescaleDB! See [Contributing](https://github.com/timescale/timescaledb/blob/main/CONTRIBUTING.md) and [Code style guide](https://github.com/timescale/timescaledb/blob/main/docs/StyleGuide.md) for details.

## Learn about Tiger Data

Tiger Data is the fastest PostgreSQL for transactional, analytical and agentic workloads. To learn more about the company and its products, visit [tigerdata.com](https://www.tigerdata.com).

## Troubleshooting

#### Docker container won't start

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

#### Can't connect with psql

- Verify Docker container is running: `docker ps`
- Check port 6543 isn't already in use: `lsof -i :6543`
- Try using explicit host: `psql -h 127.0.0.1 -p 6543 -U postgres`

#### TimescaleDB extension not found

The `timescale/timescaledb-ha:pg18` image has TimescaleDB pre-installed and pre-loaded. If you see errors, ensure you're using the correct image.

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
