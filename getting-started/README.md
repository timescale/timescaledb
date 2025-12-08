# Getting Started with TimescaleDB

Welcome to TimescaleDB! This directory contains everything you need to get started with TimescaleDB and see its performance value in under an hour.

## What is TimescaleDB?

TimescaleDB is a PostgreSQL extension for high-performance real-time analytics on time-series and event data. It combines the familiarity of PostgreSQL with specialized optimizations for time-series workloads:

- **Hybrid row-columnar storage** - 10-100x faster analytics with 90%+ compression
- **Automatic time-based partitioning** - Efficient data management at scale
- **Full SQL compatibility** - Use existing PostgreSQL tools and knowledge
- **Production-ready** - Trusted by thousands of companies worldwide

## Start Here

### [Quick Start Guide](quickstart.md) (10 minutes)

Get TimescaleDB running locally and create your first hypertable:

1. Run TimescaleDB with a single Docker command
2. Create a hypertable with columnstore enabled
3. Insert and query time-series data
4. See instant analytical performance

[Start the Quick Start Guide →](quickstart.md)

## Learn with Examples

Explore complete, standalone examples with real-world datasets:

- **[IoT Sensors](examples/iot-sensors/)** - Device monitoring patterns
- **[NYC Taxi Data](examples/nyc-taxi/)** - Transportation analytics (full example)
- **[Financial Market Data](examples/financial-ticks/)** - Trading analytics
- **[Application Events](examples/events-uuidv7/)** - Event logging with UUIDv7
- **[Cryptocurrency](examples/crypto/)** - Crypto market analysis

Each example includes:
- Complete schema definition
- Sample data (CSV files)
- Analytical queries
- Step-by-step walkthrough

[Browse all examples →](examples/)

## Bring Your Own Data

Ready to try TimescaleDB with your data?

- [Your Own Data Guide](your-own-data/) - Schema design and migration patterns

## Key Features to Explore

### 1. Hypertables with Columnstore
Create time-series tables with automatic partitioning and columnar compression:

```sql
CREATE TABLE metrics (
    time TIMESTAMPTZ NOT NULL,
    device_id TEXT NOT NULL,
    value DOUBLE PRECISION
) WITH (
    tsdb.hypertable,
    tsdb.partition_column='time',
    tsdb.segmentby='device_id'
);
```

### 2. Direct to Columnstore
Load data with instant analytical performance using COPY:

```sql
SET timescaledb.enable_direct_compress_copy = on;
COPY metrics FROM 'data.csv' WITH (FORMAT csv, HEADER true);
```

Or insert data directly to the columnstore:

```sql
SET timescaledb.enable_direct_compress_insert = on;
INSERT INTO metrics (time, device_id, value)
VALUES
    (NOW(), 'device_1', 23.5),
    (NOW(), 'device_2', 24.1),
    (NOW(), 'device_3', 22.8);
```

### 3. time_bucket() Aggregations
Powerful time-series aggregations:

```sql
SELECT
    time_bucket('1 hour', time) AS hour,
    device_id,
    AVG(value) as avg_value
FROM metrics
WHERE time > NOW() - INTERVAL '24 hours'
GROUP BY hour, device_id
ORDER BY hour DESC;
```

### 4. Continuous Aggregates
Pre-computed, auto-updating materialized views:

```sql
CREATE MATERIALIZED VIEW metrics_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS hour,
    device_id,
    AVG(value) as avg_value
FROM metrics
GROUP BY hour, device_id;
```

## Documentation & Resources

- [TimescaleDB Documentation](https://docs.timescale.com)
- [API Reference](https://docs.timescale.com/api/latest/)
- [Tutorials](https://docs.timescale.com/tutorials/latest/)
- [Community Forums](https://www.timescale.com/forum)
- [Timescale Blog](https://www.timescale.com/blog)

## Need Help?

- Check the [Quick Start troubleshooting section](quickstart.md#troubleshooting)
- Visit [Timescale Community Forums](https://www.timescale.com/forum)
- Read the [TimescaleDB Documentation](https://docs.timescale.com)
- Open an issue on [GitHub](https://github.com/timescale/timescaledb/issues)

## Production Deployment

Ready to deploy to production?

- **[Timescale Cloud](https://www.timescale.com/cloud)** - Managed TimescaleDB hosting
- **[Self-hosting Guide](https://docs.timescale.com/self-hosted/latest/)** - Deploy on your infrastructure
- **[Performance Tuning](https://docs.timescale.com/self-hosted/latest/configuration/)** - Optimize for your workload

## What's Next?

1. Complete the [Quick Start Guide](quickstart.md)
2. Try an [example](examples/) that matches your use case
3. Experiment with [your own data](your-own-data/)
4. Deploy to [Timescale Cloud](https://www.timescale.com/cloud) or [self-host](https://docs.timescale.com/self-hosted/latest/)

---

**Questions or feedback?** We'd love to hear from you in the [Timescale Community Forums](https://www.timescale.com/forum).
