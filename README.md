<div align=center>
<picture align=center>
    <source media="(prefers-color-scheme: dark)" srcset="https://assets.timescale.com/docs/images/timescale-logo-dark-mode.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://assets.timescale.com/docs/images/timescale-logo-light-mode.svg">
    <img alt="Timescale logo" >
</picture>
</div>

<div align=center>

<h3>TimescaleDB is an extension for PostgreSQL that enables time-series workloads while increasing ingest, query, storage, and analytics performance</h3>

[![Docs](https://img.shields.io/badge/Read_the_Timescale_docs-black?style=for-the-badge&logo=readthedocs&logoColor=white)](https://docs.timescale.com/)
[![SLACK](https://img.shields.io/badge/Ask_the_Timescale_community-black?style=for-the-badge&logo=slack&logoColor=white)](https://timescaledb.slack.com/archives/C4GT3N90X)
[![Try TimescaleDB for free](https://img.shields.io/badge/Try_Timescale_for_free-black?style=for-the-badge&logo=timescale&logoColor=white)](https://console.cloud.timescale.com/signup)

</div>

TimescaleDB scales PostgreSQL for time-series data with the help of [hypertables](https://docs.timescale.com/use-timescale/latest/hypertables/about-hypertables/). Hypertables are PostgreSQL tables that automatically partition your data by time and space. You interact with a hypertable in the same way as regular PostgreSQL table. Behind the scenes, the database performs the work of setting up and maintaining the hypertable's partitions.

From the perspective of both use and management, TimescaleDB looks and feels like PostgreSQL, and can be managed and queried as
such. However, it provides a range of features and optimizations that make managing your time-series data easier and more efficient.

> [!WARNING]
>
>  The latest Postgres minor releases (17.1, 16.5, 15.9, 14.14, 13.17, 12.21), released on 2024-11-14, have an unexpected
>  breaking ABI change that may crash existing deployments of TimescaleDB, unless used with a TimescaleDB binary explicitly built against those new minor PG versions.
>
>
>  Status and recommendations:
>  - **Users of [Timescale Cloud](https://console.cloud.timescale.com/) are unaffected**. We are currently not upgrading cloud databases to these latest minor PG releases. But regardless, Timescale Cloud recompiles TimescaleDB against each new minor Postgres version, which would prevent any such incompatibility.
>  - **Users to Timescale's [k8s docker image](https://github.com/timescale/timescaledb-docker-ha) are unaffected**.  We are currently not building a new release against these latest minor PG releases. But regardless, our docker image build process recompiles TimescaleDB against each new minor Postgres version, which would prevent any such incompatibility.
>  - Users of other managed clouds (using TimescaleDB Apache-2 Edition) are recommended to not upgrade to these latest minor PG releases at this time, or discuss with their cloud provider how they build TimescaleDB with new minor releases.
>  - Users who self-manage TimescaleDB are recommended to not upgrade to these latest minor PG releases at this time.
>
>  We are working with the PG community about how best to address this issue.  See [this thread on pgsql-hackers](https://www.postgresql.org/message-id/flat/CABOikdNmVBC1LL6pY26dyxAS2f%2BgLZvTsNt%3D2XbcyG7WxXVBBQ%40mail.gmail.com) for more info.
>
>  Thanks for your understanding! 🙏

**Learn more about TimescaleDB**:

- [Developer documentation](https://docs.timescale.com/)
- [Release notes](https://tsdb.co/GitHubTimescaleDocsReleaseNotes)
- [Testing TimescaleDB](test/README.md)
- [Timescale community forum](https://www.timescale.com/forum/)
- [GitHub issues](https://github.com/timescale/timescaledb/issues)
- [Timescale support](https://tsdb.co/GitHubTimescaleSupport)

**Get started with TimescaleDB**:

- [Install](#install-timescaledb)
- [Create a hypertable](#create-a-hypertable)
- [Insert and query data](#insert-and-query-data)
- [Compress data](#compress-data)
- [Create time buckets](#create-time-buckets)
- [Create continuous aggregates](#create-continuous-aggregates)
- [Back up and replicate data](#back-up-replicate-and-restore-data)

# Install TimescaleDB

Get TimescaleDB in one of the following ways:

- Install the [platform-specific package](https://docs.timescale.com/self-hosted/latest/install/).
- [Build from source](https://docs.timescale.com/self-hosted/latest/install/installation-source/).

TimescaleDB comes in the following editions: Apache 2 and Community. See the [documentation](https://docs.timescale.com/about/latest/timescaledb-editions/) for differences between them.

For reference and clarity, all code files in this repository reference [licensing](https://github.com/timescale/timescaledb/blob/main/tsl/LICENSE-TIMESCALE) in their header. Apache-2 licensed binaries can be built by passing `-DAPACHE_ONLY=1` to `bootstrap`.

PostgreSQL's out-of-the-box settings are typically too conservative for modern
servers and TimescaleDB. Make sure your `postgresql.conf`
settings are tuned, by either using [timescaledb-tune](https://github.com/timescale/timescaledb-tune)
or doing it manually.

# Create a hypertable

You create a regular table and then convert it into a hypertable.

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
SELECT create_hypertable('conditions', 'time');
```

See more:

- [About hypertables](https://docs.timescale.com/use-timescale/latest/hypertables/)
- [API reference](https://docs.timescale.com/api/latest/hypertable/)

# Insert and query data

Insert and query data in a hypertable via regular SQL commands. For example:

- Insert data into a hypertable named `conditions`:

    ```sql
    INSERT INTO conditions
      VALUES
        (NOW(), 'office', 70.0, 50.0),
        (NOW(), 'basement', 66.5, 60.0),
        (NOW(), 'garage', 77.0, 65.2);
    ```

- Return the number of entries written to the table conditions in the last 12 hours:

    ```sql
    SELECT COUNT(*) FROM conditions
      WHERE time > NOW() - INTERVAL '12 hours';
    ```

See more:

- [Query data](https://docs.timescale.com/use-timescale/latest/query-data/)
- [Write data](https://docs.timescale.com/use-timescale/latest/write-data/)

# Compress data

You compress your time-series data to reduce its size by more than 90%. This cuts storage costs and keeps your queries operating at lightning speed.

When you enable compression, the data in your hypertable is compressed chunk by chunk. When the chunk is compressed, multiple records are grouped into a single row. The columns of this row hold an array-like structure that stores all the data. This means that instead of using lots of rows to store the data, it stores the same data in a single row. Because a single row takes up less disk space than many rows, it decreases the amount of disk space required, and can also speed up your queries.

- [About compression](https://docs.timescale.com/use-timescale/latest/compression/)
- [API reference](https://docs.timescale.com/api/latest/compression/)

# Create time buckets

Time buckets enable you to aggregate data in hypertables by time interval and calculate summary values.

For example, calculate the average daily temperature in a table named `weather_conditions`. The table has a `time` and `temperature` columns:

```sql
SELECT time_bucket('1 day', time) AS bucket,
  avg(temperature) AS avg_temp
FROM weather_conditions
GROUP BY bucket
ORDER BY bucket ASC;
```

See more:

- [About time buckets](https://docs.timescale.com/use-timescale/latest/time-buckets/about-time-buckets/)
- [API reference](https://docs.timescale.com/api/latest/hyperfunctions/time_bucket/)
- [All TimescaleDB features](https://docs.timescale.com/use-timescale/latest/)
- [Tutorials](https://docs.timescale.com/tutorials/latest/)

# Create continuous aggregates

Continuous aggregates are designed to make queries on very large datasets run faster. They use PostgreSQL [materialized views](https://www.postgresql.org/docs/current/rules-materializedviews.html) to continuously and incrementally refresh a query in the background, so that when you run the query, only the data that has changed needs to be computed, not the entire dataset.

See more:

- [About continuous aggregates](https://docs.timescale.com/use-timescale/latest/continuous-aggregates/)
- [API reference](https://docs.timescale.com/api/latest/continuous-aggregates/create_materialized_view/)

# Back up, replicate, and restore data

TimescaleDB takes advantage of the reliable backup, restore, and replication functionality provided by PostgreSQL.

See more:

- [Backup and restore](https://docs.timescale.com/self-hosted/latest/backup-and-restore/)
- [Replication and high availability](https://docs.timescale.com/self-hosted/latest/replication-and-ha/)

# Want TimescaleDB hosted and managed for you? Try Timescale Cloud

[Timescale Cloud](https://docs.timescale.com/getting-started/latest/) is a cloud-based PostgreSQL platform for resource-intensive workloads. We help you build faster, scale further, and stay under budget. A Timescale Cloud service is a single optimized 100% PostgreSQL database instance that you use as is, or extend with capabilities specific to your business needs. The available capabilities are:

- Time-series and analytics: PostgreSQL with TimescaleDB. The PostgreSQL you know and love, supercharged with functionality for storing and querying time-series data at scale for analytics and other use cases. Get faster time-based queries with hypertables, continuous aggregates, and columnar storage. Save on storage with native compression, data retention policies, and bottomless data tiering to Amazon S3.
- AI and vector: PostgreSQL with vector extensions. Use PostgreSQL as a vector database with purpose built extensions for building AI applications from start to scale. Get fast and accurate similarity search with the pgvector and pgvectorscale extensions. Create vector embeddings and perform LLM reasoning on your data with the pgai extension.
- PostgreSQL: the trusted industry-standard RDBMS. Ideal for applications requiring strong data consistency, complex relationships, and advanced querying capabilities. Get ACID compliance, extensive SQL support, JSON handling, and extensibility through custom functions, data types, and extensions.
All services include all the cloud tooling you'd expect for production use: [automatic backups](https://docs.timescale.com/use-timescale/latest/backup-restore/backup-restore-cloud/), [high availability](https://docs.timescale.com/use-timescale/latest/ha-replicas/), [read replicas](https://docs.timescale.com/use-timescale/latest/ha-replicas/read-scaling/), [data forking](https://docs.timescale.com/use-timescale/latest/services/service-management/#fork-a-service), [connection pooling](https://docs.timescale.com/use-timescale/latest/services/connection-pooling/), [tiered storage](https://docs.timescale.com/use-timescale/latest/data-tiering/), [usage-based storage](https://docs.timescale.com/about/latest/pricing-and-account-management/), and much more.

# Check build status

|Linux/macOS|Linux i386|Windows|Coverity|Code Coverage|OpenSSF|
|:---:|:---:|:---:|:---:|:---:|:---:|
|[![Build Status Linux/macOS](https://github.com/timescale/timescaledb/actions/workflows/linux-build-and-test.yaml/badge.svg?branch=main&event=schedule)](https://github.com/timescale/timescaledb/actions/workflows/linux-build-and-test.yaml?query=workflow%3ARegression+branch%3Amain+event%3Aschedule)|[![Build Status Linux i386](https://github.com/timescale/timescaledb/actions/workflows/linux-32bit-build-and-test.yaml/badge.svg?branch=main&event=schedule)](https://github.com/timescale/timescaledb/actions/workflows/linux-32bit-build-and-test.yaml?query=workflow%3ARegression+branch%3Amain+event%3Aschedule)|[![Windows build status](https://github.com/timescale/timescaledb/actions/workflows/windows-build-and-test.yaml/badge.svg?branch=main&event=schedule)](https://github.com/timescale/timescaledb/actions/workflows/windows-build-and-test.yaml?query=workflow%3ARegression+branch%3Amain+event%3Aschedule)|[![Coverity Scan Build Status](https://scan.coverity.com/projects/timescale-timescaledb/badge.svg)](https://scan.coverity.com/projects/timescale-timescaledb)|[![Code Coverage](https://codecov.io/gh/timescale/timescaledb/branch/main/graphs/badge.svg?branch=main)](https://codecov.io/gh/timescale/timescaledb)|[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/8012/badge)](https://www.bestpractices.dev/projects/8012)|

# Get involved

We welcome contributions to TimescaleDB! See [Contributing](https://github.com/timescale/timescaledb/blob/main/CONTRIBUTING.md) and [Code style guide](https://github.com/timescale/timescaledb/blob/main/docs/StyleGuide.md) for details.

# Learn about Timescale

Timescale is PostgreSQL made powerful. To learn more about the company and its products, visit [timescale.com](https://www.timescale.com).




