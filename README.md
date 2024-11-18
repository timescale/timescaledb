|Linux/macOS|Linux i386|Windows|Coverity|Code Coverage|OpenSSF|
|:---:|:---:|:---:|:---:|:---:|:---:|
|[![Build Status Linux/macOS](https://github.com/timescale/timescaledb/actions/workflows/linux-build-and-test.yaml/badge.svg?branch=main&event=schedule)](https://github.com/timescale/timescaledb/actions/workflows/linux-build-and-test.yaml?query=workflow%3ARegression+branch%3Amain+event%3Aschedule)|[![Build Status Linux i386](https://github.com/timescale/timescaledb/actions/workflows/linux-32bit-build-and-test.yaml/badge.svg?branch=main&event=schedule)](https://github.com/timescale/timescaledb/actions/workflows/linux-32bit-build-and-test.yaml?query=workflow%3ARegression+branch%3Amain+event%3Aschedule)|[![Windows build status](https://github.com/timescale/timescaledb/actions/workflows/windows-build-and-test.yaml/badge.svg?branch=main&event=schedule)](https://github.com/timescale/timescaledb/actions/workflows/windows-build-and-test.yaml?query=workflow%3ARegression+branch%3Amain+event%3Aschedule)|[![Coverity Scan Build Status](https://scan.coverity.com/projects/timescale-timescaledb/badge.svg)](https://scan.coverity.com/projects/timescale-timescaledb)|[![Code Coverage](https://codecov.io/gh/timescale/timescaledb/branch/main/graphs/badge.svg?branch=main)](https://codecov.io/gh/timescale/timescaledb)|[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/8012/badge)](https://www.bestpractices.dev/projects/8012)|

TimescaleDB is an extension for PostgreSQL that enables time-series workloads, increasing ingest, query, storage, and analytics performance.

> [!WARNING]
>
>  The latest Postgres minor releases (17.1, 16.5, 15.9, 14.14, 13.17, 12.21), released 2024-11-14, have an unexpected
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

# Overview

TimescaleDB scales PostgreSQL for time-series data with the help of [hypertables](https://docs.timescale.com/use-timescale/latest/hypertables/about-hypertables/). Hypertables are PostgreSQL tables that automatically partition your data by time. You interact with a hypertable in the same way as regular PostgreSQL table. Behind the scenes, the database performs the work of setting up and maintaining the hypertable's partitions.

From the perspective of both use and management, TimescaleDB looks and feels like PostgreSQL, and can be managed and queried as
such. However, it provides a range of features and optimizations that make managing your time-series data easier and more efficient.

TimescaleDB is available as a self-hosted solution or a fully managed cloud offering (Timescale Cloud). The self-hosted TimescaleDB comes in two editions:

- Apache 2 Edition
- Community Edition

See [Documentation](https://docs.timescale.com/about/latest/timescaledb-editions/) for differences between the editions.

For reference and clarity, all code files in this repository reference [licensing](https://github.com/timescale/timescaledb/blob/main/tsl/LICENSE-TIMESCALE) in their header. Apache-2 licensed binaries can be built by passing `-DAPACHE_ONLY=1` to `bootstrap`.

TimescaleDB is pay-as-you-go. We don't charge for storage you don't use, backups, snapshots, ingress, or egress.

**Learn more about TimescaleDB**:

- [Developer documentation](https://docs.timescale.com/)
- [Release notes](https://tsdb.co/GitHubTimescaleDocsReleaseNotes)
- [Testing TimescaleDB](test/README.md)
- [Slack channel](https://slack-login.timescale.com)
- [Timescale community forum](https://www.timescale.com/forum/)
- [GitHub issues](https://github.com/timescale/timescaledb/issues)
- [Timescale support](https://tsdb.co/GitHubTimescaleSupport)

**Contribute**: We welcome contributions to TimescaleDB! See [Contributing](https://github.com/timescale/timescaledb/blob/main/CONTRIBUTING.md) and [Code style guide](https://github.com/timescale/timescaledb/blob/main/docs/StyleGuide.md) for details.

# Getting started

## Prerequisites

PostgreSQL's out-of-the-box settings are typically too conservative for modern
servers and TimescaleDB. Make sure your `postgresql.conf`
settings are tuned, by either using [timescaledb-tune](https://github.com/timescale/timescaledb-tune)
or doing it manually.

## Install TimescaleDB

Get TimescaleDB in either of the following ways:

- Install the [platform-specific package](https://docs.timescale.com/self-hosted/latest/install/).

- Get a free-trial of [Timescale Cloud](https://console.cloud.timescale.com/signup), a fully-managed TimescaleDB.

- [Build from source](https://docs.timescale.com/self-hosted/latest/install/installation-source/).

## Use TimescaleDB

You use TimescaleDB by creating regular tables, then converting them into hypertables. Once converted, you can proceed to execute regular SQL read and write operations on data, as well as using TimescaleDB-specific functions.

### Create a hypertable

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

### Insert and query data

Insert and query data in a hypertable via regular SQL commands.

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

### Use TimescaleDB-specific functions

TimescaleDB includes additional functions for time-series analysis that are not present in vanilla PostgreSQL. For example, the `time_bucket` function. Time buckets enable you to aggregate data in hypertables by time interval and calculate summary values.

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


