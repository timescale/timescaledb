# Loading NYC Taxi Data into ClickHouse

This guide shows how to load the NYC Taxi sample data into ClickHouse.

## Prerequisites

- ClickHouse installed and running
- `clickhouse-client` CLI tool
- The `nyc-taxi-sample.csv` file from the parent directory

## Method 1: Using clickhouse-client with CSV

```bash
# Connect to ClickHouse and load data directly from CSV
clickhouse-client --query="INSERT INTO trips FORMAT CSV" < ../nyc-taxi-sample.csv
```

## Method 2: Using clickhouse-client interactively

```bash
# Connect to ClickHouse
clickhouse-client

# Load the data
INSERT INTO trips FORMAT CSV INFILE '../nyc-taxi-sample.csv';
```

## Method 3: Using HTTP interface

```bash
# Load data via HTTP interface
cat ../nyc-taxi-sample.csv | curl 'http://localhost:8123/?query=INSERT%20INTO%20trips%20FORMAT%20CSV' --data-binary @-
```

## Method 4: Load from TimescaleDB directly (if both are running)

```bash
# Export from TimescaleDB
psql -h localhost -p 5432 -U postgres -c "COPY trips TO STDOUT WITH CSV HEADER" > trips_export.csv

# Import to ClickHouse
clickhouse-client --query="INSERT INTO trips FORMAT CSVWithNames" < trips_export.csv
```

## Verify Data Loaded

```sql
-- Check row count
SELECT COUNT(*) FROM trips;

-- Check data sample
SELECT * FROM trips LIMIT 10;

-- Check date range
SELECT
    MIN(pickup_datetime) as earliest,
    MAX(pickup_datetime) as latest,
    COUNT(*) as total_rows
FROM trips;
```

## Performance Notes

- ClickHouse automatically uses columnar storage with the MergeTree engine
- Data is partitioned by month using `PARTITION BY toYYYYMM(pickup_datetime)`
- The `ORDER BY (pickup_boroname, pickup_datetime)` creates a primary key for efficient queries
- No additional indexing needed - ClickHouse's sparse primary index handles most queries efficiently

## Troubleshooting

### Permission denied
If you get permission errors, ensure the CSV file path is accessible to the ClickHouse server.

### Date format issues
If dates aren't parsing correctly, ensure your CSV has dates in format: `YYYY-MM-DD HH:MM:SS`

### Schema mismatch
Ensure the CSV column order matches the table schema. Use CSVWithNames format if your CSV has headers.
