# Financial Market Data Example

This example will demonstrate financial tick and candlestick data analysis with TimescaleDB.
The datasets corresponds to the stocks listed in the [S&P 500 index](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies), with fictive prices and movements.
The tick cadence is per second, over three business days, containing approx 35 million records in total and 503 tickers.

## Use Cases

Trading platforms, market data analysis, portfolio analytics, algorithmic trading

## What You'll Learn

- OHLCV (Open, High, Low, Close, Volume) data modeling
- Candlestick aggregations at multiple intervals
- Continuous aggregates for different timeframes (1min, 5min, 1hour)
- Real-time market analysis queries

## Dataset Preview

* timestamp
* ticker
* price
* price delta
* change percentage
* volume

```csv
2025-11-12 14:30:00+00:00,NVDA,38.25,0.25,0.65359,5095712
2025-11-12 14:30:00+00:00,AAPL,152.03,0.03,0.01973,6466554
2025-11-12 14:30:00+00:00,MSFT,129.23,0.23,0.17798,4417848
2025-11-12 14:30:00+00:00,GOOG,174.93,-0.07,-0.04002,19602229
2025-11-12 14:30:00+00:00,GOOGL,71.21,0.21,0.2949,3149482
2025-11-12 14:30:00+00:00,AMZN,95.95,-0.05,-0.05211,12150474
2025-11-12 14:30:00+00:00,AVGO,196.15,0.15,0.07647,6166047
2025-11-12 14:30:00+00:00,META,133.22,0.22,0.16514,12230004
2025-11-12 14:30:00+00:00,TSLA,82.0,0.0,0.0,10298937
2025-11-12 14:30:00+00:00,BRK.B,56.84,-0.16,-0.28149,10980047
```

## Prerequisites

- Docker installed
- `psql` PostgreSQL client
- 15-20 minutes

## Quick Start

First download the dataset:

```sh
curl -L https://assets.timescale.com/timescaledb-datasets/sp500_stock_prices_3d_1s.tar.gz | tar -xzf - 
```

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

```sh
psql "postgres://postgres:password@localhost:6543/postgres"
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
-- Create the stock_prices table with the column ts as partitioning
-- Note: for optimized query performance grouping on ticker, we select this column to segment by
CREATE TABLE stock_prices (
  ts                TIMESTAMPTZ         NOT NULL,
  ticker            TEXT                NOT NULL,
  price             DOUBLE PRECISION    NOT NULL,
  change_delta      DOUBLE PRECISION    NOT NULL,
  change_percentage DOUBLE PRECISION    NOT NULL,
  volume            BIGINT NOT NULL CHECK (volume >= 0)
)
WITH (
  timescaledb.hypertable,
  timescaledb.segmentby='ticker'
);
```

This creates a `stock_prices` table with:
- partitioned by timestamp on column `ts`
- Segmentation by `ticker` for optimal compression and query performance

### Step 4: Load Sample Data into TimescaleDB

This approach writes data directly to the columnstore, bypassing the rowstore entirely. You get instant analytical performance.

**From psql:**

```sql
-- Enable direct to columnstore for this session
SET timescaledb.enable_direct_compress_copy = on;

-- Load data directly into columnstore (if you have a CSV file)
\COPY stock_prices FROM 'sp500_stock_prices_3d_1s.csv' WITH (FORMAT csv, HEADER true);

-- Verify data loaded
SELECT COUNT(*) FROM stock_prices;
```

### Step 5: Run Sample Queries

Now let's explore the data with some analytical queries. Run these in your `psql` session:

```sql
-- Activate time measuring
\timing on
```

**Query 1: OHLCV per hour of AAPL**

Aggregating raw 1 second tick data into 15-minute "candlesticks" (Open, High, Low, Close, Volume) of the ticker `AAPL`.

```sql
SELECT
    time_bucket('1 hour', ts) AS hour_bucket,
    ticker,
    FIRST(price, ts) AS open_price,
    MAX(price) AS high_price,
    MIN(price) AS low_price,
    LAST(price, ts) AS close_price,
    AVG(price) AS avg_price,
    SUM(volume) AS sum_volume
FROM
    stock_prices
WHERE 
    ticker = 'AAPL'
GROUP BY
    hour_bucket,
    ticker
ORDER BY
    hour_bucket DESC;
```

**Query 2: Trend Analysis: Simple Moving Average (SMA) of MSFT**

Calculating a "smoothing" line to see trends over noise for the ticker `MSFT` over 4 hours.

```sql
WITH candles AS (
  SELECT
    time_bucket('1 hour', ts) AS bucket,
    ticker,
    LAST(price, ts) AS close_price
  FROM stock_prices
  WHERE ticker = 'MSFT'
  GROUP BY bucket, ticker
)
SELECT
  bucket,
  ticker,
  close_price,
  AVG(close_price) OVER (
    PARTITION BY ticker 
    ORDER BY bucket 
    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
  ) AS sma_4hours
FROM candles
ORDER BY bucket DESC;
```

**Query 3: Hour-over-Hour Return**

Comparing the current price to the price exactly one hour ago to calculate percentage growth.

```sql
WITH hourly_close AS (
  SELECT
    time_bucket('1 hour', ts) AS bucket,
    ticker,
    LAST(price, ts) AS closing_price
  FROM stock_prices
  GROUP BY bucket, ticker
)
SELECT
  bucket,
  ticker,
  closing_price,
  LAG(closing_price, 1) OVER (PARTITION BY ticker ORDER BY bucket) AS prev_close,
  ((closing_price - LAG(closing_price, 1) OVER (PARTITION BY ticker ORDER BY bucket)) 
   / LAG(closing_price, 1) OVER (PARTITION BY ticker ORDER BY bucket)) * 100 AS hourly_return_pct
FROM hourly_close;
```

**Query 4: Price volatility**

```sql
SELECT
  ticker,
  AVG(price) AS avg_price,
  STDDEV(price) AS price_volatility,
  MAX(price) - MIN(price) AS price_spread
FROM stock_prices
WHERE ts > NOW() - INTERVAL '7 days'
GROUP BY ticker
HAVING count(*) > 10
ORDER BY price_volatility DESC;
```

## What's Happening Behind the Scenes?

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

The `tsdb.segmentby='ticker'` setting:
- Groups data by user within each chunk
- Improves compression ratios (similar data together)
- Speeds up queries that filter by ticker
- Better for ticker-based analytics

## Continuous Aggregates (Advanced)

For real-time dashboards, you can create continuous aggregates that automatically update:

```sql
-- Create a continuous aggregate for hourly candlesticks
CREATE MATERIALIZED VIEW candlesticks_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', ts) AS hour,
    ticker,
    FIRST(price, ts) AS open_price,
    MAX(price) AS high_price,
    MIN(price) AS low_price,
    LAST(price, ts) AS close_price,
    AVG(price) AS avg_price,
    SUM(volume) AS sum_volume
FROM stock_prices
GROUP BY hour, ticker
ORDER BY hour DESC, ticker ASC;

-- Add a refresh policy to keep it updated
SELECT add_continuous_aggregate_policy('candlesticks_hourly',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
```

Now you can query `candlesticks_hourly` for instant results on pre-aggregated data.

```sql
SELECT * from candlesticks_hourly WHERE ticker = 'NFLX';
```

---

**Questions?** Check out [TimescaleDB Documentation](https://docs.timescale.com) or the [TimescaleDB Community Forums](https://www.timescale.com/forum).
