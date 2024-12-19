<div align=center>
<picture align=center>
    <source media="(prefers-color-scheme: dark)" srcset="https://assets.timescale.com/docs/images/timescale-logo-dark-mode.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://assets.timescale.com/docs/images/timescale-logo-light-mode.svg">
    <img alt="Timescale logo" >
</picture>
</div>

<div align=center>

<h3>TimescaleDB is a PostgreSQL extension for high-performance real-time analytics on time-series and event data</h3>

[![Docs](https://img.shields.io/badge/Read_the_Timescale_docs-black?style=for-the-badge&logo=readthedocs&logoColor=white)](https://docs.timescale.com/)
[![SLACK](https://img.shields.io/badge/Ask_the_Timescale_community-black?style=for-the-badge&logo=slack&logoColor=white)](https://timescaledb.slack.com/archives/C4GT3N90X)
[![Try TimescaleDB for free](https://img.shields.io/badge/Try_Timescale_for_free-black?style=for-the-badge&logo=timescale&logoColor=white)](https://console.cloud.timescale.com/signup)

</div>

## Install TimescaleDB

Install from a Docker container:

1. Run the TimescaleDB container:

    ```bash
    docker run -d --name timescaledb -p 5432:5432 -e POSTGRES_PASSWORD=password timescale/timescaledb-ha:pg17
    ```

1. Connect to a database:

    ```bash
    docker exec -it timescaledb psql -d "postgres://postgres:password@localhost/postgres"
    ```

See [other installation options](https://docs.timescale.com/self-hosted/latest/install/) or try [Timescale Cloud](https://docs.timescale.com/getting-started/latest/) for free.

## Create a hypertable

You create a regular table and then convert it into a hypertable. A hypertable automatically partitions data into chunks based on your configuration.

```sql
-- Create timescaledb extension
CREATE EXTENSION timescaledb;

-- Create a regular SQL table
CREATE TABLE conditions (
  time        TIMESTAMPTZ       NOT NULL,
  location    TEXT              NOT NULL,
  temperature DOUBLE PRECISION  NULL,
  humidity    DOUBLE PRECISION  NULL
);

-- Convert the table into a hypertable that is partitioned by time
SELECT create_hypertable('conditions', by_range('time'));
```

See more:

- [About hypertables](https://docs.timescale.com/use-timescale/latest/hypertables/)
- [API reference](https://docs.timescale.com/api/latest/hypertable/)

## Enable columnstore

TimescaleDB's hypercore is a hybrid row-columnar store that boosts analytical query performance on your time-series and event data, while reducing data size by more than 90%. This keeps your queries operating at lightning speed and ensures low storage costs as you scale. Data is inserted in row format in the rowstore and converted to columnar format in the columnstore based on your configuration.

- Configure the columnstore on a hypertable:

    ```sql
    ALTER TABLE conditions SET (
      timescaledb.compress,
      timescaledb.compress_segmentby = 'time'
    );
    ```

- Create a policy to automatically convert chunks in row format that are older than seven days to chunks in the columnar format:

    ```sql
    SELECT add_compression_policy('conditions', INTERVAL '7 days');
    ```

See more:

- [About columnstore](https://docs.timescale.com/use-timescale/latest/compression/about-compression/)
- [Enable columnstore manually](https://docs.timescale.com/use-timescale/latest/compression/manual-compression/)
- [API reference](https://docs.timescale.com/api/latest/compression/)

## Insert and query data

Insert and query data in a hypertable via regular SQL commands. For example:

- Insert data into a hypertable named `conditions`:

    ```sql
    INSERT INTO conditions
      VALUES
        (NOW(), 'office',   70.0, 50.0),
        (NOW(), 'basement', 66.5, 60.0),
        (NOW(), 'garage',   77.0, 65.2);
    ```

- Return the number of entries written to the table conditions in the last 12 hours:

    ```sql
    SELECT
      COUNT(*)
    FROM
      conditions
    WHERE
      time > NOW() - INTERVAL '12 hours';
    ```

See more:

- [Query data](https://docs.timescale.com/use-timescale/latest/query-data/)
- [Write data](https://docs.timescale.com/use-timescale/latest/write-data/)

## Create time buckets

Time buckets enable you to aggregate data in hypertables by time interval and calculate summary values.

For example, calculate the average daily temperature in a table named `conditions`. The table has a `time` and `temperature` columns:

```sql
SELECT
  time_bucket('1 day', time) AS bucket,
  AVG(temperature) AS avg_temp
FROM
  conditions
GROUP BY
  bucket
ORDER BY
  bucket ASC;
```

See more:

- [About time buckets](https://docs.timescale.com/use-timescale/latest/time-buckets/about-time-buckets/)
- [API reference](https://docs.timescale.com/api/latest/hyperfunctions/time_bucket/)
- [All TimescaleDB features](https://docs.timescale.com/use-timescale/latest/)
- [Tutorials](https://docs.timescale.com/tutorials/latest/)

## Create continuous aggregates

Continuous aggregates are designed to make queries on very large datasets run faster. They continuously and incrementally refresh a query in the background, so that when you run such query, only the data that has changed needs to be computed, not the entire dataset. This is what makes them different from regular PostgreSQL [materialized views](https://www.postgresql.org/docs/current/rules-materializedviews.html), which cannot be incrementally materialized and have to be rebuilt from scratch every time you want to refresh it.

For example, create a continuous aggregate view for daily weather data in two simple steps:

1. Create a materialized view:

   ```sql
   CREATE MATERIALIZED VIEW conditions_summary_daily
   WITH (timescaledb.continuous) AS
   SELECT
     location,
     time_bucket(INTERVAL '1 day', time) AS bucket,
     AVG(temperature),
     MAX(temperature),
     MIN(temperature)
   FROM
     conditions
   GROUP BY
     location,
     bucket;
   ```

1. Create a policy to refresh the view every hour:

   ```sql
   SELECT
     add_continuous_aggregate_policy(
       'conditions_summary_daily',
       start_offset => INTERVAL '1 month',
       end_offset => INTERVAL '1 day',
       schedule_interval => INTERVAL '1 hour'
   );
   ```
See more:

- [About continuous aggregates](https://docs.timescale.com/use-timescale/latest/continuous-aggregates/)
- [API reference](https://docs.timescale.com/api/latest/continuous-aggregates/create_materialized_view/)

## Want TimescaleDB hosted and managed for you? Try Timescale Cloud

[Timescale Cloud](https://docs.timescale.com/getting-started/latest/) is a cloud-based PostgreSQL platform for resource-intensive workloads. We help you build faster, scale further, and stay under budget. A Timescale Cloud service is a single optimized 100% PostgreSQL database instance that you use as is, or extend with capabilities specific to your business needs. The available capabilities are:

- **Time-series and analytics**: PostgreSQL with TimescaleDB. The PostgreSQL you know and love, supercharged with functionality for storing and querying time-series data at scale for analytics and other use cases. Get faster time-based queries with hypertables, continuous aggregates, and columnar storage. Save on storage with native compression, data retention policies, and bottomless data tiering to Amazon S3.
- **AI and vector**: PostgreSQL with vector extensions. Use PostgreSQL as a vector database with purpose built extensions for building AI applications from start to scale. Get fast and accurate similarity search with the pgvector and pgvectorscale extensions. Create vector embeddings and perform LLM reasoning on your data with the pgai extension.
- **PostgreSQL**: the trusted industry-standard RDBMS. Ideal for applications requiring strong data consistency, complex relationships, and advanced querying capabilities. Get ACID compliance, extensive SQL support, JSON handling, and extensibility through custom functions, data types, and extensions.
All services include all the cloud tooling you'd expect for production use: [automatic backups](https://docs.timescale.com/use-timescale/latest/backup-restore/backup-restore-cloud/), [high availability](https://docs.timescale.com/use-timescale/latest/ha-replicas/), [read replicas](https://docs.timescale.com/use-timescale/latest/ha-replicas/read-scaling/), [data forking](https://docs.timescale.com/use-timescale/latest/services/service-management/#fork-a-service), [connection pooling](https://docs.timescale.com/use-timescale/latest/services/connection-pooling/), [tiered storage](https://docs.timescale.com/use-timescale/latest/data-tiering/), [usage-based storage](https://docs.timescale.com/about/latest/pricing-and-account-management/), and much more.

## Check build status

|Linux/macOS|Linux i386|Windows|Coverity|Code Coverage|OpenSSF|
|:---:|:---:|:---:|:---:|:---:|:---:|
|[![Build Status Linux/macOS](https://github.com/timescale/timescaledb/actions/workflows/linux-build-and-test.yaml/badge.svg?branch=main&event=schedule)](https://github.com/timescale/timescaledb/actions/workflows/linux-build-and-test.yaml?query=workflow%3ARegression+branch%3Amain+event%3Aschedule)|[![Build Status Linux i386](https://github.com/timescale/timescaledb/actions/workflows/linux-32bit-build-and-test.yaml/badge.svg?branch=main&event=schedule)](https://github.com/timescale/timescaledb/actions/workflows/linux-32bit-build-and-test.yaml?query=workflow%3ARegression+branch%3Amain+event%3Aschedule)|[![Windows build status](https://github.com/timescale/timescaledb/actions/workflows/windows-build-and-test.yaml/badge.svg?branch=main&event=schedule)](https://github.com/timescale/timescaledb/actions/workflows/windows-build-and-test.yaml?query=workflow%3ARegression+branch%3Amain+event%3Aschedule)|[![Coverity Scan Build Status](https://scan.coverity.com/projects/timescale-timescaledb/badge.svg)](https://scan.coverity.com/projects/timescale-timescaledb)|[![Code Coverage](https://codecov.io/gh/timescale/timescaledb/branch/main/graphs/badge.svg?branch=main)](https://codecov.io/gh/timescale/timescaledb)|[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/8012/badge)](https://www.bestpractices.dev/projects/8012)|

## Get involved

We welcome contributions to TimescaleDB! See [Contributing](https://github.com/timescale/timescaledb/blob/main/CONTRIBUTING.md) and [Code style guide](https://github.com/timescale/timescaledb/blob/main/docs/StyleGuide.md) for details.

## Learn about Timescale

Timescale is PostgreSQL made powerful. To learn more about the company and its products, visit [timescale.com](https://www.timescale.com).

