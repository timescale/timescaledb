<div align=center>
<picture align=center>
    <source media="(prefers-color-scheme: dark)" srcset="https://assets.timescale.com/docs/images/timescale-logo-dark-mode.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://assets.timescale.com/docs/images/timescale-logo-light-mode.svg">
    <img alt="Timescale logo" >
</picture>
</div>

<div align=center>

<h3>TimescaleDB is an extension for PostgreSQL that enables time-series, events, and real-time analytics workloads, while increasing ingest, query, and storage performance</h3>

[![Docs](https://img.shields.io/badge/Read_the_Timescale_docs-black?style=for-the-badge&logo=readthedocs&logoColor=white)](https://docs.timescale.com/)
[![SLACK](https://img.shields.io/badge/Ask_the_Timescale_community-black?style=for-the-badge&logo=slack&logoColor=white)](https://timescaledb.slack.com/archives/C4GT3N90X)
[![Try TimescaleDB for free](https://img.shields.io/badge/Try_Timescale_for_free-black?style=for-the-badge&logo=timescale&logoColor=white)](https://console.cloud.timescale.com/signup)

</div>

TimescaleDB scales PostgreSQL for ingesting and querying vast amounts of live data. From the perspective of both use and management, TimescaleDB looks and feels just like PostgreSQL, and can be managed and queried as such. However, it provides a wide range of features and optimizations that supercharge your queries - all while keeping the costs down. For example, our hybrid row-columnar engine makes queries up to 350x faster, ingests 44% faster, and reduces storage by 95%. Visit [timescale.com](https://www.timescale.com) for details, use cases, customer success stories, and more.

<table style="width:100%;">
<thead>
  <tr>
    <th width="500px">Get started with TimescaleDB</th>
    <th width="500px">Learn more about TimescaleDB</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td><ul><li><a href="https://github.com/timescale/timescaledb/tree/main?tab=readme-ov-file#install-timescaledb">Install</a></li><li><a href="https://github.com/timescale/timescaledb/tree/main?tab=readme-ov-file#create-a-hypertable">Create a hypertable</a></li><li><a href="https://github.com/timescale/timescaledb/tree/main?tab=readme-ov-file#insert-and-query-data">Insert and query data</a></li><li><a href="https://github.com/timescale/timescaledb/tree/main?tab=readme-ov-file#compress-data">Compress data</a></li><li><a href="https://github.com/timescale/timescaledb/tree/main?tab=readme-ov-file#create-time-buckets">Create time buckets</a></li><li><a href="https://github.com/timescale/timescaledb/tree/main?tab=readme-ov-file#create-continuous-aggregates">Create continuous aggregates</a></li><li><a href="https://github.com/timescale/timescaledb/tree/main?tab=readme-ov-file#back-up-replicate-and-restore-data">Back up and replicate data</a></li></ul></td>
    <td><ul><li><a href="https://docs.timescale.com/">Developer documentation</a></li><li><a href="https://tsdb.co/GitHubTimescaleDocsReleaseNotes">Release notes</a></li><li><a href="https://github.com/timescale/timescaledb/blob/main/test/README.md">Testing TimescaleDB</a></li><li><a href="https://www.timescale.com/forum/">Timescale community forum</a></li><li><a href="https://github.com/timescale/timescaledb/issues">GitHub issues</a></li><li><a href="https://tsdb.co/GitHubTimescaleSupport">Timescale support</a></li></ul></td>
  </tr>
</tbody>
</table>

## Install TimescaleDB

Installation options are:

- **Platform packages**: TimescaleDB is also available pre-packaged for several platforms such as
  Linux, Windows, MacOS, Docker, and Kubernetes. For more information, see [Install TimescaleDB](https://docs.timescale.com/self-hosted/latest/install/).

- **Build from source**: See [Building from source](https://docs.timescale.com/self-hosted/latest/install/installation-source/).

   We recommend not using TimescaleDB with PostgreSQL 17.1, 16.5, 15.9, 14.14, 13.17, 12.21.
   These minor versions [introduced a breaking binary interface change](https://www.postgresql.org/about/news/postgresql-172-166-1510-1415-1318-and-1222-released-2965/) that,
   once identified, was reverted in subsequent minor PostgreSQL versions 17.2, 16.6, 15.10, 14.15, 13.18, and 12.22.
   When you build from source, best practice is to build with PostgreSQL 17.2, 16.6, etc and higher.
   Users of [Timescale Cloud](https://console.cloud.timescale.com/) and Platform packages built and
   distributed by Timescale are unaffected.

- **[Timescale Cloud](https://tsdb.co/GitHubTimescale)**: A fully-managed TimescaleDB in the cloud, is
  available via a free trial. Create a PostgreSQL database in the cloud with TimescaleDB pre-installed
  so you can power your application with TimescaleDB without the management overhead. [Learn more](#want-timescaledb-hosted-and-managed-for-you-try-timescale-cloud) about Timescale Cloud.

TimescaleDB comes in the following editions: Apache 2 and Community. See the [documentation](https://docs.timescale.com/about/latest/timescaledb-editions/) for differences between them. For reference and clarity, all code files in this repository reference [licensing](https://github.com/timescale/timescaledb/blob/main/tsl/LICENSE-TIMESCALE) in their header. Apache-2 licensed binaries can be built by passing `-DAPACHE_ONLY=1` to `bootstrap`.

## Create a hypertable

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
SELECT create_hypertable('conditions', by_range('time'));
```

See more:

- [About hypertables](https://docs.timescale.com/use-timescale/latest/hypertables/)
- [API reference](https://docs.timescale.com/api/latest/hypertable/)

## Insert and query data

Insert and query data in a hypertable via regular SQL commands. For example:

- Insert data into a hypertable named `conditions`:

    ```sql
    INSERT INTO conditions
      VALUES
        (pg_catalog.now(), 'office',   70.0, 50.0),
        (pg_catalog.now(), 'basement', 66.5, 60.0),
        (pg_catalog.now(), 'garage',   77.0, 65.2);
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

## Compress data

You compress your time-series data to reduce its size by more than 90%. This cuts storage costs and keeps your queries operating at lightning speed.

When you enable compression, the data in your hypertable is compressed chunk by chunk. When the chunk is compressed, multiple records are grouped into a single row. The columns of this row hold an array-like structure that stores all the data. This means that instead of using lots of rows to store the data, it stores the same data in a single row. Because a single row takes up less disk space than many rows, it decreases the amount of disk space required, and can also speed up your queries. For example:

- Enable compression on hypertable

    ```sql
    ALTER TABLE conditions SET (
      timescaledb.compress,
      timescaledb.compress_segmentby = 'device_id'
    );
    ```
- Compress hypertable chunks manually:

    ```sql
    SELECT
      compress_chunk(chunk, if_not_compressed => TRUE)
    FROM
      show_chunks(
        'conditions',
        pg_catalog.now() - INTERVAL '1 week',
        pg_catalog.now() - INTERVAL '3 weeks'
      ) AS chunk;
    ```

- Create a policy to compress chunks that are older than seven days automatically:

    ```sql
    SELECT add_compression_policy('conditions', INTERVAL '7 days');
    ```

See more:

- [About compression](https://docs.timescale.com/use-timescale/latest/compression/)
- [API reference](https://docs.timescale.com/api/latest/compression/)

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
     device,
     time_bucket(INTERVAL '1 day', time) AS bucket,
     AVG(temperature),
     MAX(temperature),
     MIN(temperature)
   FROM
     conditions
   GROUP BY
     device,
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

## Back up, replicate, and restore data

TimescaleDB takes advantage of the reliable backup, restore, and replication functionality provided by PostgreSQL.

See more:

- [Backup and restore](https://docs.timescale.com/self-hosted/latest/backup-and-restore/)
- [Replication and high availability](https://docs.timescale.com/self-hosted/latest/replication-and-ha/)

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

