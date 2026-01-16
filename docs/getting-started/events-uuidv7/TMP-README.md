# App Events Data for TimescaleDB UUIDv7 Tutorial

## Overview

This is a dataset of **15 million app events** that simulates a typical SaaS/e-commerce application's analytics data:

- **UUIDv7 partitioning** - Partition hypertables by time-embedded UUIDs (**no separate timestamp column needed!**)
- **Boundary queries** - Efficient time filtering with `to_uuidv7_boundary()`
- **Vectorized UUID compression** - 30% storage savings, 2x query performance
- **Multi-column SkipScan** - Fast DISTINCT queries across user/session
- **Columnstore** - Automatic compression with configurable sparse indexes

**Key Innovation:** The table has NO separate `event_time` column - the timestamp is embedded directly in the UUIDv7 `event_id`!

## Loading Data into TimescaleDB

### 1. Create the Schema

Connect to your TimescaleDB instance:

```sql
-- Create the hypertable with UUIDv7 partitioning
-- 🎯 KEY POINT: No separate timestamp column needed - time is embedded in event_id!
-- 🎯 KEY POINT: No PRIMARY KEY to allow direct compress during COPY
CREATE TABLE app_events (
    event_id UUID NOT NULL,
    user_id UUID NOT NULL,
    session_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    event_name TEXT,
    device_type TEXT,
    country_code TEXT,
    properties JSONB,
    revenue_cents BIGINT
) WITH (
    tsdb.hypertable,
    tsdb.partition_column = 'event_id',
    tsdb.segmentby = 'user_id'
);

-- Create index on event_id for lookups (not unique to allow direct compress)
CREATE INDEX idx_app_events_event_id ON app_events(event_id);
```

### 2. Load Data with Direct Compress

```bash
# Connect to your TimescaleDB database
psql -d your_database
```

```sql
-- Enable direct compress for this session
SET timescaledb.enable_direct_compress_copy = on;

-- Load directly from CSV - data goes straight to compressed chunks!
\copy app_events FROM PROGRAM 'gzip -dc app_events_test.csv.gz' WITH (FORMAT csv, HEADER true);

-- Or for uncompressed CSV:
-- SET timescaledb.enable_direct_compress_copy = on;
-- \copy app_events FROM 'app_events_test.csv' WITH (FORMAT csv, HEADER true);
```


### 3. Verify the Data

```sql
-- Check row count
SELECT COUNT(*) FROM app_events;

-- View chunk distribution
SELECT
    chunk_name,
    range_start,
    range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'app_events'
ORDER BY range_start;

-- View compression stats (if compression is enabled)
SELECT * FROM chunk_compression_stats('app_events');

-- Sample data with extracted timestamp
SELECT
    event_id,
    uuid_timestamp(event_id) as event_time,  -- Extract time from UUID!
    event_type,
    user_id
FROM app_events
LIMIT 10;
```

---

## Dataset Characteristics

### Event Distribution

| Event Type   | Percentage | ~Count (15M) | Description |
|--------------|------------|--------------|-------------|
| page_view    | ~58%       | 8.7M         | User views a page |
| click        | ~28%       | 4.2M         | User clicks an element |
| form_submit  | ~4%        | 600K         | Form submission |
| add_to_cart  | ~5%        | 750K         | Product added to cart |
| purchase     | ~2%        | 300K         | Completed purchase |
| signup       | ~1%        | 150K         | New user registration |

### Cardinalities

| Dimension    | Count   | Notes |
|--------------|---------|-------|
| Users        | 500K    | Power-law session distribution |
| Sessions     | ~2M     | Avg 7-8 events per session |
| Countries    | 26      | Weighted toward US, GB, DE |
| Devices      | 3       | mobile (58%), desktop (35%), tablet (7%) |
| Time span    | 90 days | Creates ~13 chunks (7-day default) |

### Realistic Patterns

- **Funnel progression**: Sessions follow browsing → engaged → cart → purchase stages
- **Time distribution**: Higher traffic during business hours (10am-4pm, 8pm-10pm)
- **Weekday bias**: ~30% less traffic on weekends
- **User behavior**: Power-law distribution (some users very active, most have few sessions)
- **Revenue**: Purchase events include realistic order values by product category

---

## Example Queries

### 1. Efficient Time Range Query (Chunk Pruning)

```sql
-- ✅ CORRECT: Uses boundary function for chunk exclusion
EXPLAIN ANALYZE
SELECT COUNT(*), event_type
FROM app_events
WHERE event_id >= to_uuidv7_boundary(now() - interval '7 days')
GROUP BY event_type;
```

**Why this is fast:** The `to_uuidv7_boundary()` function creates a UUID boundary value that TimescaleDB can use to exclude entire chunks without scanning them.

### 2. Inefficient Query (Anti-Pattern)

```sql
-- ❌ WRONG: Scans ALL chunks, extracts timestamp from every row
EXPLAIN ANALYZE
SELECT COUNT(*), event_type
FROM app_events
WHERE uuid_timestamp(event_id) >= now() - interval '7 days'
GROUP BY event_type;
```

**Why this is slow:** The `uuid_timestamp()` function must be evaluated for every row, preventing chunk exclusion.

### 3. Multi-Column SkipScan (Latest Event per User/Session)

```sql
-- Demonstrates 90x faster DISTINCT with SkipScan
EXPLAIN ANALYZE
SELECT DISTINCT ON (user_id, session_id)
    user_id,
    session_id,
    event_type,
    uuid_timestamp(event_id) as event_time
FROM app_events
WHERE event_id >= to_uuidv7_boundary(now() - interval '30 days')
ORDER BY user_id, session_id, event_id DESC;
```

### 4. Funnel Analysis

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

### 5. Revenue by Country (Last 30 Days)

```sql
SELECT
    country_code,
    COUNT(*) as purchase_count,
    SUM(revenue_cents) / 100.0 as total_revenue,
    AVG(revenue_cents) / 100.0 as avg_order_value
FROM app_events
WHERE event_id >= to_uuidv7_boundary(now() - interval '30 days')
  AND event_type = 'purchase'
GROUP BY country_code
ORDER BY total_revenue DESC
LIMIT 10;
```

### 6. Inspect Chunks and Compression

```sql
-- View chunk distribution
SELECT
    chunk_name,
    range_start,
    range_end,
    pg_size_pretty(total_bytes) as total_size,
    pg_size_pretty(table_bytes) as table_size,
    pg_size_pretty(index_bytes) as index_size
FROM timescaledb_information.chunks
WHERE hypertable_name = 'app_events'
ORDER BY range_start;

-- Compression statistics
SELECT * FROM hypertable_columnstore_stats('app_events');

-- Columnstore settings
SELECT * FROM timescaledb_information.hypertable_columnstore_settings
WHERE hypertable_name = 'app_events';
```


## Key UUIDv7 Functions

| Function | Description |
|----------|-------------|
| `gen_random_uuidv7()` | Generate a new UUIDv7 with current timestamp |
| `uuidv7(timestamptz)` | Generate UUIDv7 from specific timestamp |
| `uuid_timestamp(uuid)` | Extract timestamp from UUIDv7 |
| `to_uuidv7_boundary(timestamptz)` | Create boundary UUID for range queries |
| `to_uuidv7_boundary(timestamptz, bool)` | `true` = lower bound (zeros), `false` = upper bound (ones) |

---