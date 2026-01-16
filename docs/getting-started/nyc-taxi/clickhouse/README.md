# ClickHouse Version - NYC Taxi Example

This directory contains the ClickHouse version of the NYC Taxi example for comparison with TimescaleDB.

## Quick Start

### 1. Start ClickHouse

Using Docker:
```bash
docker run -d --name clickhouse-server \
    -p 8123:8123 \
    -p 9000:9000 \
    clickhouse/clickhouse-server
```

### 2. Create the Schema

```bash
# Using clickhouse-client
clickhouse-client < clickhouse-schema.sql

# Or connect interactively
clickhouse-client
# Then paste the contents of clickhouse-schema.sql
```

### 3. Load Data

See [LOAD_DATA.md](LOAD_DATA.md) for detailed loading instructions.

Simple method:
```bash
clickhouse-client --query="INSERT INTO trips FORMAT CSV" < ../nyc-taxi-sample.csv
```

### 4. Run Queries

```bash
# Run all queries
clickhouse-client < clickhouse-queries.sql

# Or run individual queries interactively
clickhouse-client
```

## Key Differences from TimescaleDB

### Storage Model
- **ClickHouse**: Columnar storage by default with MergeTree engine
- **TimescaleDB**: Hybrid row-columnar storage (hypercore)

### Time Functions
- **ClickHouse**: Uses `toStartOfHour()`, `toDate()`, etc.
- **TimescaleDB**: Uses `time_bucket()` function

### Partitioning
- **ClickHouse**: `PARTITION BY toYYYYMM(pickup_datetime)` - partitions by month
- **TimescaleDB**: `tsdb.partition_column='pickup_datetime'` - automatic time-based chunks

### Ordering/Primary Key
- **ClickHouse**: `ORDER BY (pickup_boroname, pickup_datetime)` creates sparse primary index
- **TimescaleDB**: `tsdb.orderby='pickup_datetime DESC'` plus explicit indexes

### Data Loading
- **ClickHouse**: Multiple methods (CSV, HTTP, native format)
- **TimescaleDB**: `COPY` with optional direct-to-columnstore mode

## Files

- `clickhouse-schema.sql` - Table schema for ClickHouse
- `clickhouse-queries.sql` - Sample analytical queries
- `LOAD_DATA.md` - Data loading instructions
- `README.md` - This file

## Comparison Purpose

This ClickHouse example allows you to:
1. Compare query syntax between TimescaleDB and ClickHouse
2. Test query performance on the same dataset
3. Understand differences in time-series database approaches
4. Evaluate which system fits your use case better

## Note

The TimescaleDB version is in the parent directory. Use the same CSV data file for both systems to ensure fair comparison.
