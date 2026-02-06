# TimescaleDB Examples

This directory contains complete, standalone examples to help you get started with TimescaleDB using real-world use cases. Each example includes sample data and analytical queries to showcase TimescaleDB's capabilities.

## Available Examples

### [NYC Taxi Data](nyc-taxi/)
Analyze New York City taxi trip data to understand transportation patterns. Great for learning about location-based analytics and high-volume time-series data.

**What you'll learn:**
- Handling high-cardinality data (locations, cab types)
- Time-series aggregations with `time_bucket()`
- Segmentation strategies for optimal compression
- Revenue and usage pattern analysis

**Use cases:** Transportation analytics, ride-sharing platforms, logistics optimization, urban planning

---

### [Financial Market Data](financial-ticks/)
Work with financial tick and candlestick data for market analysis. Ideal for understanding high-frequency time-series data and multi-timeframe aggregations.

**What you'll learn:**
- OHLCV (Open, High, Low, Close, Volume) data modeling
- Creating candlestick aggregations at multiple intervals
- Continuous aggregates for different timeframes (1min, 5min, 1hour)
- Real-time market analysis queries

**Use cases:** Trading platforms, market data analysis, portfolio analytics, algorithmic trading

---

### [Application Events with UUIDv7](events-uuidv7/)
Track and analyze application events using modern UUIDv7 identifiers. Perfect for understanding event-driven analytics and user behavior tracking.

**What you'll learn:**
- Using UUIDv7 for time-ordered unique identifiers
- Efficient time-range queries with `to_uuidv7_boundary()`
- Session tracking and user analytics
- Event funnel and conversion analysis

**Use cases:** Application monitoring, user behavior analytics, audit logging, event-driven architectures

---

## Workshops

### [AI Workshop: EV Charging Station Analysis](https://github.com/timescale/TigerData-Workshops/tree/main/AI-Workshop)
Integrate PostgreSQL with AI capabilities for managing and analyzing EV charging station data. This workshop demonstrates how to combine time-series data with vector search and AI features.

**What you'll learn:**
- Integrating TimescaleDB with AI and vector extensions
- Managing EV charging station time-series data
- Vector search and similarity queries
- AI-powered analytics on time-series data

**Use cases:** EV infrastructure management, smart grid analytics, energy optimization, predictive maintenance

---

### [Time-Series Workshop: Financial Data Analysis](https://github.com/timescale/TigerData-Workshops/tree/main/TimeSeries-Workshop-Finance/)
Work with cryptocurrency tick data and create candlestick charts. Learn advanced time-series analysis techniques for financial markets.

**What you'll learn:**
- Working with high-frequency cryptocurrency tick data
- Creating candlestick aggregations and visualizations
- Advanced time-series analysis patterns
- Multi-timeframe financial analytics

**Use cases:** Cryptocurrency trading, market analysis, financial data visualization, algorithmic trading

---

## How to Use These Examples

Each example is completely standalone and self-contained. You can use any example as your starting point:

1. **Choose an example** that matches your use case or interests
2. **Navigate to the example directory** and read the README
3. **Follow the step-by-step guide** - each example includes:
   - Complete schema definition (SQL)
   - Sample data (CSV) included in the repository
   - Data loading instructions (both direct to columnstore and standard approaches)
   - Sample analytical queries (SQL)
   - Detailed explanations of what's happening

4. **Run the queries** and see TimescaleDB's columnstore performance in action
5. **Adapt to your needs** - use these patterns for your own data

## Quick Start Path

**New to TimescaleDB?** We recommend this path:

1. Start with the [Quick Start Guide](../../README.md#quick-start-with-timescaledb) (10 minutes)
2. Try the [NYC Taxi](nyc-taxi/) example (20 minutes)
3. Explore other examples based on your use case
4. Ready for your data? See [Your Own Data Guide](../your-own-data/)

## Common Patterns Across All Examples

All examples demonstrate these TimescaleDB features:

- **Hypertables** - Automatic time-based partitioning with `tsdb.hypertable`
- **Columnstore** - Hybrid row-columnar storage with `tsdb.enable_columnstore=true`
- **Direct to Columnstore** - Instant analytical performance with `enable_direct_compress_copy`
- **time_bucket()** - Powerful time-series aggregation function
- **Compression** - Automatic 90%+ data compression
- **Optimal indexing** - Best practices for time-series indexes

## Prerequisites

All examples require:
- Docker (for running TimescaleDB)
- A PostgreSQL client (`psql` recommended)
- 10-30 minutes of your time

Each example works with the same Docker setup. You have two options:

**Option 1: One-line install (Recommended)**

```sh
curl -sL https://tsdb.co/start-local | sh
```

This command:
- Downloads and starts TimescaleDB (if not already downloaded)
- Exposes PostgreSQL on port **6543** (a non-standard port to avoid conflicts with other PostgreSQL instances on port 5432)
- Automatically tunes settings for your environment using timescaledb-tune
- Sets up a persistent data volume

**Option 2: Manual Docker command**

```bash
docker run -d --name timescaledb \
    -p 6543:5432 \
    -e POSTGRES_PASSWORD=password \
    timescale/timescaledb-ha:pg18
```

**Note:** We use port **6543** (mapped to container port 5432) to avoid conflicts if you have other PostgreSQL instances running on the standard port 5432.

## Example Selection Guide

**Choose your example based on your use case:**

| Your Use Case | Recommended Example |
|--------------|---------------------|
| Location-based services, GPS tracking | [NYC Taxi](nyc-taxi/) |
| Financial trading, market data | [Financial Ticks](financial-ticks/) |
| Application logs, user events | [Events with UUIDv7](events-uuidv7/) |
| Cryptocurrency, volatile markets | [Time-Series Workshop: Financial Data Analysis](https://github.com/timescale/TigerData-Workshops/tree/main/TimeSeries-Workshop-Finance/) |
| AI/ML with time-series data | [AI Workshop: EV Charging Station Analysis](https://github.com/timescale/TigerData-Workshops/tree/main/AI-Workshop) |
| General time-series analytics | Start with [NYC Taxi](nyc-taxi/) |

## What Makes These Examples Different?

- **Completely standalone** - No dependencies between examples
- **Sample data included** - CSV files in the repo, ready to load
- **Production-ready patterns** - Real-world schema designs and query patterns
- **Instant performance** - Direct to columnstore examples for immediate results
- **Copy-paste friendly** - All code works as-is in `psql`
- **Explained thoroughly** - Comments and documentation explain the "why"

## Next Steps

After trying these examples:

1. **Bring your own data** - See [Your Own Data Guide](../your-own-data/)
2. **Learn advanced features** - Explore [TimescaleDB Documentation](https://docs.timescale.com)
3. **Production deployment** - Check out [Timescale Cloud](https://www.timescale.com/cloud) for managed hosting
4. **Join the community** - Get help in [Timescale Community Forums](https://www.timescale.com/forum)

## Contributing

Found an issue or want to improve an example? Contributions are welcome! Please open an issue or pull request on our [GitHub repository](https://github.com/timescale/timescaledb).

---

**Ready to start?** Pick an example above and dive in!
