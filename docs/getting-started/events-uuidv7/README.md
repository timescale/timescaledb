# Application Events with UUIDv7 Example

Get started with TimescaleDB using application event data leveraging UUIDv7 identifiers. This example demonstrates how to handle event logging and analytics using time-embedded UUIDs for partitioning—**no separate timestamp column needed!**

## What You'll Learn

- Using UUIDv7 for time-ordered unique identifiers
- Efficient time-range queries with `to_uuidv7_boundary()`
- Session tracking and user analytics
- Event funnel and conversion analysis

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
-- Create the app_events table with UUIDv7 partitioning
-- Note: No separate timestamp column needed - the timestamp is embedded in the UUIDv7!
-- Note: No PRIMARY KEY to allow direct compress during COPY
CREATE TABLE IF NOT EXISTS app_events (
    event_id UUID NOT NULL,
    user_id UUID NOT NULL,
    session_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    event_name TEXT,
    device_type TEXT,
    country_code TEXT,
    category TEXT,
    page_path TEXT,
    referrer TEXT,
    viewport_width INTEGER,
    element_id TEXT,
    position_x INTEGER,
    position_y INTEGER,
    product_id TEXT,
    quantity INTEGER,
    revenue_cents INTEGER
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'event_id',
    tsdb.segmentby = 'user_id'
);

-- Create index on event_id for lookups (not unique to allow direct compress)
CREATE INDEX idx_app_events_event_id ON app_events(event_id);
```

This creates an `app_events` table with:
- UUIDv7 partitioning on `event_id` (time is embedded in the UUID)
- Segmentation by `user_id` for optimal compression
- No separate timestamp column needed!

### Step 4: Load Sample Data

First, download and decompress the sample data:

```bash
# Download the sample data
wget https://assets.timescale.com/timescaledb-datasets/events_uuid.csv.gz

# Decompress the CSV file
gunzip events_uuid.csv.gz

# This will create events_uuid.csv ready for loading
```

We provide two approaches for loading data. Choose based on your needs:

#### Option A: Direct to Columnstore (Recommended - Instant Performance)

This approach writes data directly to the columnstore, bypassing the rowstore entirely. You get instant analytical performance.

**From command line:**

```bash
psql -h localhost -p 6543 -U postgres \
  -v ON_ERROR_STOP=1 \
  -c "SET timescaledb.enable_direct_compress_copy = on;
      COPY app_events FROM STDIN WITH (FORMAT csv, HEADER true);" \
  < events_uuid.csv
```

This command reads the CSV file from your local filesystem and pipes it to PostgreSQL, which loads it directly into the columnstore.

**Verify data loaded:**

```sql
SELECT COUNT(*) FROM app_events;
```

#### Option B: Standard COPY (Fallback)

This approach loads data into the rowstore first. Data will be converted to the columnstore by a background policy (12-24 hours) for faster querying.

**From command line:**

```bash
psql -h localhost -p 6543 -U postgres \
  -v ON_ERROR_STOP=1 \
  -c "COPY app_events FROM STDIN WITH (FORMAT csv, HEADER true);" \
  < events_uuid.csv
```

**Verify data loaded:**

```sql
SELECT COUNT(*) FROM app_events;
```

**Manually convert to columnstore (Optional):**

If you loaded data using standard copy a background process will convert your rowstore data to the columnstore in 12-24 hours, you can manually convert it immediately to get the best query performance:

```sql
DO $$
DECLARE ch TEXT;
BEGIN
    FOR ch IN SELECT show_chunks('app_events') LOOP
        CALL convert_to_columnstore(ch);
    END LOOP;
END $$;
```

### Step 5: Run Sample Queries

Now let's explore the data with some analytical queries. Run these in your `psql` session:

**Query 1: Efficient Time Range Query (Chunk Pruning)**
```sql
-- ✅ CORRECT: Uses boundary function for chunk exclusion
\timing on
SELECT COUNT(*), event_type
FROM app_events
WHERE event_id >= to_uuidv7_boundary(now() - interval '7 days')
GROUP BY event_type;
```

**Why this is fast:** The `to_uuidv7_boundary()` function creates a UUID boundary value that TimescaleDB can use to exclude entire chunks without scanning them.

**Query 2: Inefficient Query (Anti-Pattern)**
```sql
-- ❌ WRONG: Scans ALL chunks, extracts timestamp from every row
SELECT COUNT(*), event_type
FROM app_events
WHERE uuid_timestamp(event_id) >= now() - interval '7 days'
GROUP BY event_type;
```

**Why this is slow:** The `uuid_timestamp()` function must be evaluated for every row, preventing chunk exclusion.

**Query 3: SkipScan on Single Column (Distinct Users)**
```sql
-- Demonstrates SkipScan optimization: uses compression index to skip repeated values
\timing on
SELECT DISTINCT ON (user_id)
    user_id,
    event_type,
    uuid_timestamp(event_id) as event_time
FROM app_events
WHERE event_id >= to_uuidv7_boundary(now() - interval '30 days')
ORDER BY user_id, event_id DESC
LIMIT 50;
```

**Why this uses SkipScan:** Since `user_id` is the segmentby column, TimescaleDB automatically creates a compression index on it. SkipScan can jump directly to the next unique `user_id` value instead of scanning all rows. The WHERE clause ensures chunk exclusion (only scans chunks with recent data), making it even faster.

**Verify SkipScan is used:** Check the query plan with `EXPLAIN` - you should see `Custom Scan (SkipScan)` on the compressed chunks instead of a sequential scan.

**Query 4: Funnel Analysis**
```sql
WITH funnel AS (
    SELECT
        user_id,
        session_id,
        MAX(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) as viewed,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) as added_to_cart,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchased
    FROM app_events
    WHERE event_id >= to_uuidv7_boundary(now() - interval '30 days')
    GROUP BY user_id, session_id
)
SELECT
    SUM(viewed) as sessions_with_view,
    SUM(added_to_cart) as sessions_with_cart,
    SUM(purchased) as sessions_with_purchase,
    ROUND(100.0 * SUM(added_to_cart) / NULLIF(SUM(viewed), 0), 2) as view_to_cart_pct,
    ROUND(100.0 * SUM(purchased) / NULLIF(SUM(added_to_cart), 0), 2) as cart_to_purchase_pct
FROM funnel;
```

**Query 5: Revenue by Country (Last 30 Days)**
```sql
SELECT
    country_code,
    COUNT(*) as purchase_count,
    SUM(revenue_cents) / 100.0 as total_revenue,
    ROUND(AVG(revenue_cents) / 100.0, 2) as avg_order_value
FROM app_events
WHERE event_id >= to_uuidv7_boundary(now() - interval '30 days')
  AND event_type = 'purchase'
GROUP BY country_code
ORDER BY total_revenue DESC
LIMIT 10;
```

## What's Happening Behind the Scenes?

### UUIDv7 Partitioning

When you create a table with `tsdb.partition_column = 'event_id'` where `event_id` is a UUIDv7:
- TimescaleDB automatically partitions your data by the time-embedded UUID
- **No separate timestamp column needed** - the timestamp is embedded in the UUID itself
- Chunk exclusion works with `to_uuidv7_boundary()` for efficient time-range queries
- Vectorized UUID compression provides 30% storage savings and 2x query performance

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

### Segmentation

The `tsdb.segmentby='user_id'` setting:
- Groups data by user within each chunk
- Improves compression ratios (similar data together)
- Speeds up queries that filter by user_id
- Better for user-based analytics

### UUIDv7 Functions

TimescaleDB provides comprehensive UUIDv7 functionality across **all supported PostgreSQL versions** (including PostgreSQL 15, 16, and 17), while PostgreSQL only provides UUIDv7 support in PostgreSQL 18. 

For complete documentation on all available UUIDv7 functions, see the [UUIDv7 Functions API Reference](https://www.tigerdata.com/docs/api/latest/uuid-functions). 

## Continuous Aggregates (Advanced)

For real-time dashboards, you can create continuous aggregates that automatically update:

```sql
-- Create a continuous aggregate for hourly event statistics
CREATE MATERIALIZED VIEW app_events_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', uuid_timestamp(event_id)) AS hour,
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users,
    SUM(revenue_cents) / 100.0 as total_revenue
FROM app_events
GROUP BY hour, event_type;

-- Add a refresh policy to keep it updated
SELECT add_continuous_aggregate_policy('app_events_hourly',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
```

Now you can query `app_events_hourly` for instant results on pre-aggregated data.

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
- Verify you're using `to_uuidv7_boundary()` for time-range queries (not `uuid_timestamp()`)
- Check if data is compressed: `SELECT * FROM timescaledb_information.chunks WHERE hypertable_name = 'app_events';`
- Ensure chunk exclusion is working: Use `EXPLAIN ANALYZE` to see chunk pruning

### UUIDv7 functions not found
- Verify TimescaleDB version supports UUIDv7: `SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';`
- UUIDv7 support requires TimescaleDB 2.24+ with the uuidv7 extension enabled

## Use Cases

This Application Events example demonstrates patterns applicable to:

- **SaaS analytics** - User behavior tracking, feature usage, conversion funnels
- **E-commerce** - Shopping cart analysis, purchase patterns, product recommendations
- **Application monitoring** - Error tracking, performance metrics, user sessions
- **Audit logging** - Security events, compliance tracking, change history
- **Event-driven architectures** - Microservices event sourcing, message queues
- **A/B testing** - Experiment tracking, variant analysis, statistical significance

## Files in This Example

- [`README.md`](README.md) - This file
- Sample data files (to be added)

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

---

**Questions?** Check out [TimescaleDB Documentation](https://docs.timescale.com) or the [TimescaleDB Community Forums](https://www.timescale.com/forum).
