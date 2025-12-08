# Cursor/Claude Code Prompt: TimescaleDB Day-0 Getting Started Experience

## Context
You are working on the TimescaleDB GitHub repository (https://github.com/timescale/timescaledb). The goal is to completely revamp the getting started experience to help new users see TimescaleDB's performance value in under 1 hour, directly addressing competitive losses to ClickHouse.

## Problem Statement
- **Current Issue**: Users experience "basic Postgres performance" during evaluation because data takes 7+ days to reach the columnstore by default
- **Competitive Gap**: 0% win rate vs ClickHouse in Q3 2024 for deals >$50k ARR
- **Root Cause**: Setup complexity, delayed columnstore access, unclear documentation path
- **Goal**: Get users to "aha moment" with columnstore performance in first hour

## Key Product Features to Showcase
1. **Hypercore Columnstore** - Hybrid row-columnar engine (10-100x faster analytics, 90%+ compression)
2. **Direct Compress** - New feature that writes data directly to columnstore (instant performance)
3. **Automatic Hypertable Creation** - Simple `WITH (tsdb.hypertable)` syntax
4. **Continuous Aggregates** - Pre-computed, auto-updating materialized views
5. **Zero-config defaults** - Automatic partition_column, segmentby, orderby selection

## Technical Requirements

### Repository Structure to Create
```
timescaledb/
├── README.md (UPDATE - revamp completely)
├── getting-started/
│   ├── README.md (NEW - overview and index)
│   ├── quickstart.md (NEW - 10-minute quick start)
│   ├── examples/
│   │   ├── README.md (NEW - examples index)
│   │   ├── nyc-taxi/
│   │   │   ├── README.md (NEW - complete standalone walkthrough)
│   │   │   ├── nyc-taxi-schema.sql (NEW - optimized hypertable schema)
│   │   │   ├── nyc-taxi-sample.csv (NEW - sample dataset ~1000 rows)
│   │   │   └── nyc-taxi-queries.sql (NEW - sample analytical queries)
│   │   ├── iot-sensors/
│   │   │   ├── README.md (NEW - complete standalone walkthrough)
│   │   │   ├── iot-schema.sql (NEW)
│   │   │   ├── iot-sample-data.csv (NEW - sample sensor data)
│   │   │   └── iot-queries.sql (NEW)
│   │   ├── financial-ticks/
│   │   │   ├── README.md (NEW - complete standalone walkthrough)
│   │   │   ├── financial-schema.sql (NEW)
│   │   │   ├── financial-sample-data.csv (NEW - sample market data)
│   │   │   └── financial-queries.sql (NEW)
│   │   ├── events-uuidv7/
│   │   │   ├── README.md (NEW - complete standalone walkthrough)
│   │   │   ├── events-schema.sql (NEW)
│   │   │   ├── events-sample-data.csv (NEW - sample events with UUIDs)
│   │   │   └── events-queries.sql (NEW)
│   │   └── crypto/
│   │       ├── README.md (NEW - complete standalone walkthrough)
│   │       ├── crypto-schema.sql (NEW)
│   │       ├── crypto-sample-data.csv (NEW - sample crypto data)
│   │       └── crypto-queries.sql (NEW)
│   └── your-own-data/
│       ├── README.md (NEW - guide for bringing own data)
│       ├── schema-design-guide.md (don't create this yet - will be part of main docs not here)
│       └── migration-guide.md (don't create this yet - will be part of main docs not here)
```
```

## Step-by-Step Implementation Plan

### Phase 1: Update Main README.md
**File: README.md**

**Approach: Integrate new sections smoothly, maintain existing style**

Instead of a complete rewrite, integrate these new elements while preserving the existing README structure and tone:

1. **Add a "Quick Start" section** (after the intro, before deep technical content)
   - Simple Docker command
   - Link to getting-started/quickstart.md
   - Keep it brief (3-4 lines of code max)

2. **Add an "Examples" section** (near the end, before Community/License)
   - Brief intro paragraph
   - List of 5 domain examples with links
   - One sentence description per example
   - Maintain existing README formatting style

3. **Enhance existing "What is TimescaleDB?" or feature sections**
   - Add mention of direct to columnstore capability
   - Update any outdated syntax to TimescaleDB 2.24+ (tsdb.* parameters)
   - Keep existing explanations, just refresh examples

4. **Update existing code examples** if present
   - Use new `tsdb.hypertable` syntax
   - Show `tsdb.enable_columnstore=true`
   - Update any deprecated function calls

**What NOT to change:**
- Don't change the overall structure/flow
- Don't change the writing tone
- Don't remove existing sections
- Don't add performance numbers/benchmarks
- Keep existing badges, links, and formatting style
- Maintain the same level of technical detail

**Goal:** Make it feel like a natural evolution, not a revolution. Someone familiar with the existing README should feel right at home.

### Phase 2: Create Quick Start Guide
**File: getting-started/quickstart.md**

Must include:
1. **Prerequisites**: Docker, 8GB RAM, curl
2. **Step 1 - Start TimescaleDB (30 seconds)**: Single docker run command
3. **Step 2 - Connect (10 seconds)**: psql connection command
4. **Step 3 - Create First Hypertable**: Complete working example with the new syntax:
   ```sql
   CREATE TABLE sensor_data (
       time TIMESTAMPTZ NOT NULL,
       sensor_id TEXT NOT NULL,
       temperature DOUBLE PRECISION,
       humidity DOUBLE PRECISION
   ) WITH (
       tsdb.hypertable,
       tsdb.partition_column='time',
       tsdb.enable_columnstore=true,
       tsdb.segmentby='sensor_id'
   );
   ```
4. **Step 4 - Insert Sample Data**: Quick INSERT statements to test
5. **Step 5 - Run Your First Query**: Show columnstore in action
6. **Next Steps**: Links to full examples, production deployment, bringing own data

### Phase 3: Create NYC Taxi Example (Standalone)
**Directory: getting-started/examples/nyc-taxi/**

Each example must be completely standalone and self-contained.

**README.md** should include:
- Overview: "Get started with TimescaleDB using NYC taxi trip data"
- What You'll Learn (3-4 bullets)
- Prerequisites (just Docker)
- Quick Start with numbered steps showing:
  1. Create the hypertable
  2. Load sample data using direct to columnstore (primary method)
  3. Alternative: Load using standard COPY (fallback if direct to columnstore unavailable)
  4. Run sample queries
- Sample queries showcase (5-10 queries with explanations)
- What's Happening section explaining columnstore, compression, performance
- Troubleshooting section
- Next steps

**schema.sql**:
```sql
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
```

**sample-data.csv**:
Include ~1000 rows of actual NYC taxi data for quick testing. This should be committed to the repo so users can immediately test without downloading anything.

**README.md** should show two loading approaches:

**Approach 1: Direct to Columnstore (Recommended - Instant Performance)**
```sql
-- Enable direct to columnstore for this session
SET timescaledb.enable_direct_compress_copy = on;

-- Load data directly into columnstore for instant performance
-- This bypasses the rowstore entirely
\COPY trips FROM 'sample-data.csv' WITH (FORMAT csv, HEADER true);

-- Verify data loaded
SELECT COUNT(*) FROM trips;
```

**Approach 2: Standard COPY (Fallback)**
```sql
-- Standard COPY without direct to columnstore
-- Data will be compressed by background policy (takes longer)
\COPY trips FROM 'sample-data.csv' WITH (FORMAT csv, HEADER true);

-- Verify data loaded
SELECT COUNT(*) FROM trips;

-- Optional: Manually convert data to the columnstore instead of waiting for policy
SELECT compress_chunk(chunk) FROM show_chunks('trips');
```

**Note:** Direct to columnstore can also be used from the command line:
```bash
psql -h localhost -p 5432 -U postgres \
  -v ON_ERROR_STOP=1 \
  -c "SET timescaledb.enable_direct_compress_copy = on;
      COPY trips FROM STDIN WITH (FORMAT csv, HEADER true);" \
  < sample-data.csv
```

**queries.sql**:
```sql
-- Sample analytical queries for NYC Taxi dataset
-- These showcase TimescaleDB's columnstore performance

\echo '=== Sample Queries for NYC Taxi Data ==='
\echo ''

-- Query 1: Total trips and revenue
\echo 'Query 1: Overall statistics'
SELECT 
    COUNT(*) as total_trips,
    SUM(fare_amount) as total_revenue,
    AVG(fare_amount) as avg_fare,
    AVG(trip_distance) as avg_distance
FROM trips;

-- Query 2: Trips by cab type
\echo ''
\echo 'Query 2: Breakdown by cab type'
SELECT 
    cab_type,
    COUNT(*) as trips,
    AVG(fare_amount) as avg_fare,
    AVG(tip_amount) as avg_tip,
    AVG(passenger_count) as avg_passengers
FROM trips
GROUP BY cab_type
ORDER BY trips DESC;

-- Query 3: Hourly patterns using time_bucket
\echo ''
\echo 'Query 3: Hourly trip patterns (using time_bucket)'
SELECT 
    time_bucket('1 hour', pickup_datetime) AS hour,
    COUNT(*) as trips,
    AVG(fare_amount) as avg_fare,
    SUM(tip_amount) as total_tips
FROM trips
GROUP BY hour
ORDER BY hour DESC
LIMIT 24;

-- Query 4: Top pickup locations by revenue
\echo ''
\echo 'Query 4: Top pickup locations by revenue'
SELECT 
    pickup_location_id,
    COUNT(*) as trip_count,
    SUM(fare_amount) as total_revenue,
    AVG(trip_distance) as avg_distance,
    AVG(tip_amount) as avg_tip
FROM trips
GROUP BY pickup_location_id
ORDER BY total_revenue DESC
LIMIT 10;

-- Query 5: Daily aggregation with time_bucket
\echo ''
\echo 'Query 5: Daily statistics by cab type'
SELECT 
    time_bucket('1 day', pickup_datetime) AS day,
    cab_type,
    COUNT(*) as trips,
    AVG(fare_amount) as avg_fare,
    MAX(fare_amount) as max_fare
FROM trips
GROUP BY day, cab_type
ORDER BY day DESC, cab_type
LIMIT 20;

-- Query 6: Distance-based analysis
\echo ''
\echo 'Query 6: Trips by distance category'
SELECT 
    CASE 
        WHEN trip_distance < 1 THEN 'Short (< 1 mile)'
        WHEN trip_distance < 5 THEN 'Medium (1-5 miles)'
        WHEN trip_distance < 10 THEN 'Long (5-10 miles)'
        ELSE 'Very Long (> 10 miles)'
    END as distance_category,
    COUNT(*) as trips,
    AVG(fare_amount) as avg_fare,
    AVG(tip_amount) as avg_tip
FROM trips
GROUP BY distance_category
ORDER BY trips DESC;

\echo ''
\echo '=== Query examples complete! ==='
\echo 'Notice how fast these analytical queries run on compressed columnar data.'
```

### Phase 4: Create IoT Sensors Example (Standalone)
**Directory: getting-started/examples/iot-sensors/**

Complete standalone example focusing on device metrics and monitoring patterns.

**Structure:**
- README.md with complete walkthrough
- schema.sql optimized for IoT metrics
- sample-data.csv with realistic sensor readings (~1000-2000 rows)
- queries.sql showing monitoring and alerting patterns

**Key features to showcase:**
- Multiple metrics per device
- Time-series aggregation for monitoring
- Real-time continuous aggregates for dashboards
- Downsampling patterns

**Sample data should include:** device_id, timestamp, temperature, humidity, cpu_usage, battery_level, etc.

### Phase 5: Create Financial Ticks Example (Standalone)
**Directory: getting-started/examples/financial-ticks/**

Complete standalone example focusing on market data and trading analytics.

**Structure:**
- README.md with complete walkthrough
- schema.sql optimized for tick/candle data
- sample-data.csv with historical market data
- queries.sql showing financial analysis patterns

**Key features to showcase:**
- OHLCV (Open, High, Low, Close, Volume) data
- Candlestick aggregation with time_bucket
- Continuous aggregates for different timeframes (1min, 5min, 1hour)
- Market analysis queries

### Phase 6: Create Events with UUIDv7 Example (Standalone)
**Directory: getting-started/examples/events-uuidv7/**

Complete standalone example focusing on application event logs with modern UUID standards.

**Structure:**
- README.md with complete walkthrough
- schema.sql optimized for event data with UUIDv7
- sample-data.csv with realistic event data (~1000-2000 rows)
- queries.sql showing event analysis patterns

**Key features to showcase:**
- UUIDv7 for naturally time-ordered unique identifiers
- JSON metadata for flexible event attributes
- Session tracking and user analytics
- Event funnel analysis

**Sample data should include:** event_id (UUIDv7), timestamp, user_id, event_type, session_id, metadata (JSONB), etc.

### Phase 7: Document Docker Setup
**Update: getting-started/quickstart.md**

The Docker setup should be extremely simple - just a single `docker run` command.

**Key points:**
- Use the official `timescale/timescaledb-ha:pg17` image
- The image includes timescaledb-tune which runs automatically
- No custom Dockerfile or docker-compose needed
- Settings are automatically tuned for the host environment

**Example Docker command:**
```bash
docker run -d --name timescaledb \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD=password \
    timescale/timescaledb-ha:pg17
```

That's it! TimescaleDB is ready to use with:
- Automatic environment-based tuning via timescaledb-tune
- All extensions pre-installed
- Production-ready configuration
- Columnstore enabled by default

### Phase 8: Your Own Data Guide
**Directory: getting-started/your-own-data/**

Create guides to help users bring their own data:
1. **README.md**: Overview of the process
2. **schema-design-guide.md**: How to design optimal schemas for time-series data
3. **migration-guide.md**: Migrating from vanilla Postgres to TimescaleDB

Focus on:
- Simple step-by-step instructions
- Common patterns and best practices
- Examples of good vs bad schema designs
- How to choose partition_column, segmentby, orderby

## Code Style Guidelines

1. **SQL Files**:
   - Add comments explaining each section
   - Use consistent indentation (4 spaces)
   - Include `\timing on` for showing query performance
   - Add `\echo` statements for user feedback
   - Use TimescaleDB 2.24+ syntax (new `tsdb.*` parameters)
   - Show both direct to columnstore and standard COPY examples
   - Explain what's happening behind the scenes in comments
   - **CRITICAL**: Direct to columnstore requires `SET timescaledb.enable_direct_compress_copy = on;` before COPY
   
   Example direct to columnstore in psql:
   ```sql
   SET timescaledb.enable_direct_compress_copy = on;
   \COPY table FROM 'file.csv' WITH (FORMAT csv, HEADER true);
   ```
   
   Example from command line:
   ```bash
   psql -h localhost -U postgres \
     -c "SET timescaledb.enable_direct_compress_copy = on;
         COPY table FROM STDIN WITH (FORMAT csv, HEADER true);" \
     < file.csv
   ```

2. **Markdown Files**:
   - Use clear heading hierarchy (# ## ### ####)
   - Include code blocks with language tags
   - Keep paragraphs short (2-3 sentences)
   - Use tables for comparison data
   - Include "What You'll Learn" sections
   - Add "Next Steps" at the end
   - Link generously to other docs

3. **README Files**:
   - Start with one-sentence summary
   - Include prerequisites section (just Docker)
   - Use numbered steps for tutorials
   - Show both direct to columnstore and standard COPY approaches
   - Add "What's Happening" section explaining columnstore benefits
   - Add troubleshooting section
   - Include expected query patterns/results
   - Provide clear next steps
   - Each example must be completely standalone

## Key Messages to Convey

1. **"See value in first hour"** - Users should have fast queries running within 60 minutes
2. **"PostgreSQL you know"** - Full SQL compatibility, familiar tools, no learning curve
3. **"Instant columnstore performance"** - Direct to columnstore means no waiting for background jobs
4. **"Simple to get started"** - One Docker command, simple SQL syntax to create hypertables
5. **"Production ready"** - Show it scales, has enterprise features, trusted by companies

## Critical DO NOTs

1. ❌ Don't mention competitors (ClickHouse, InfluxDB, etc.) by name in docs
2. ❌ Don't make users wait - use direct to columnstore for instant access
3. ❌ Don't overwhelm with options - provide opinionated defaults, show fallbacks
4. ❌ Don't skip the "why" - explain benefits of columnstore, compression, time_bucket
5. ❌ Don't forget mobile users - keep markdown clean and readable
6. ❌ Don't use deprecated syntax - only use TimescaleDB 2.24+ API:
   - ✅ Use: `tsdb.enable_columnstore=true`
   - ❌ Avoid: `timescaledb.compress=true`
   - ✅ Use: `tsdb.segmentby`
   - ❌ Avoid: `timescaledb.compress_segmentby`
   - ✅ Use: `add_columnstore_policy()`
   - ❌ Avoid: `add_compression_policy()`
7. ❌ Don't use shell scripts for data loading - show SQL COPY commands directly
8. ❌ Don't require external tools (DuckDB, parquet-tools) - keep examples self-contained
9. ❌ Don't include benchmarks in first version - focus on getting started
10. ❌ Don't make examples dependent on each other - each must be standalone
11. ❌ Don't forget to SET timescaledb.enable_direct_compress_copy = on before using direct to columnstore
12. ❌ Don't use fake COPY syntax like `DIRECT_COMPRESS true` - it doesn't exist
13. ❌ Don't use emojis in documentation

## Testing Checklist

Before considering this complete, test:
- [ ] Fresh Docker install works from README (single docker run command)
- [ ] Quick start guide completes in <10 minutes
- [ ] Each example is completely standalone and works independently
- [ ] Direct to columnstore COPY commands work correctly
- [ ] Standard COPY fallback examples work correctly
- [ ] All sample queries run and show expected patterns
- [ ] Links work correctly (no 404s)
- [ ] Code blocks are copy-pasteable
- [ ] Each example has sample-data.csv included in repo (no generators)
- [ ] Sample data is realistic and demonstrates the use case well (~1000-2000 rows)
- [ ] All SQL syntax is valid for TimescaleDB 2.24+
- [ ] Mobile rendering looks good in GitHub
- [ ] No external dependencies (DuckDB, parquet-tools, etc.) required
- [ ] Examples can be swapped as the "primary" example easily
- [ ] No emojis in any documentation

## Success Metrics

This implementation is successful if:
1. A new user can get TimescaleDB running in <2 minutes (single docker run command)
2. They can load sample data and run queries within 10 minutes
3. They see instant columnstore performance with direct to columnstore
4. They understand how to bring their own data within 30 minutes
5. The main README is clean, simple, and competitive with alternatives
6. Each example is self-contained and can work as the "primary" example
7. Documentation is clear enough that support questions decrease
8. No external tools or dependencies required
9. Examples demonstrate real-world use cases users can relate to
10. Documentation is professional and emoji-free

## Implementation Order

I recommend implementing in this order:
1. ✅ getting-started/quickstart.md (create the new entry point first)
2. ✅ getting-started/examples/README.md (index of all examples)
3. ✅ Pick ONE complete example to start (e.g., iot-sensors or financial-ticks)
   - Complete README.md
   - schema.sql
   - sample-data.csv
   - queries.sql
4. ✅ Main README.md integration (add Quick Start and Examples sections)
   - Review existing README structure first
   - Integrate new sections smoothly
   - Update any existing code examples to 2.24+ syntax
5. ⚠️ Additional domain examples (each completely standalone)
6. ⚠️ getting-started/your-own-data/ guide
7. ⚠️ Migration and schema design guides

**Important for Phase 4:** Before touching the main README.md, read it carefully to understand the existing style, tone, and structure. The goal is seamless integration, not replacement.

## Final Notes

- This is a **simplified, focused rewrite** of the getting started experience
- Every example must be **completely standalone** - no dependencies on other examples
- Focus on **showing value immediately** - direct to columnstore means instant performance
- Use **simple SQL commands** - no shell scripts, no external tools
- Keep it **copy-paste friendly** - users should be able to follow along in psql
- **Compete on simplicity** - one Docker command, simple SQL, instant results
- Think like a **new user**, not an engineer who already knows TimescaleDB
- Examples should use **real sample data** included in the repo as CSV files
- Each example should be **swappable as the primary** - don't hardcode "NYC Taxi" as special
- **No benchmarks in v1** - focus purely on getting started, showing patterns, running queries
- **No data generators** - just include pre-generated sample CSV files (~1000-2000 rows each)
- Sample data should be **realistic and demonstrate the use case** clearly
- **No emojis** - keep documentation professional and clean

---

## Your Task

Please implement this Day-0 getting started experience for TimescaleDB following this specification. Start with the getting-started/quickstart.md (Phase 1), then create one complete example (Phase 3), and finally integrate the new Quick Start and Examples sections into the main README.md (Phase 4) - being careful to preserve its existing style and structure.

For each file you create:
1. Follow the style guidelines above
2. Include complete, working code (no TODOs unless absolutely necessary)
3. Add helpful comments explaining what's happening
4. Test that syntax is correct for TimescaleDB 2.24+
5. Ensure all links and cross-references work
6. Make examples completely standalone
7. Show both direct to columnstore and standard COPY approaches
8. Include sample CSV data in the repo (~1000-2000 rows, realistic data)
9. Keep documentation professional and emoji-free

**For the main README.md specifically:**
- Read and understand the existing structure first
- Integrate new sections smoothly (don't replace entire README)
- Match the existing tone and formatting style
- Update code examples to use 2.24+ syntax where they exist
- Make it feel like a natural evolution, not a revolution

When you're ready, show me the new getting-started/quickstart.md first for review before proceeding with the other files.