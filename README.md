[![Build Status](https://travis-ci.org/timescale/timescaledb.svg?branch=master)](https://travis-ci.org/timescale/timescaledb)

## TimescaleDB

TimescaleDB is an open-source database designed to make SQL scalable for
time-series data. It is engineered up from PostgreSQL, providing automatic
partitioning across time and space (partitioning key), as well as full
SQL support.

TimescaleDB is packaged as a PostgreSQL extension and released under
the Apache 2 open-source license. [Contributors welcome.](https://github.com/timescale/timescaledb/blob/master/CONTRIBUTING.md)

Below is an introduction to TimescaleDB. For more information, please check out these other resources:
- [Developer Documentation](http://docs.timescale.com/)
- [Slack Channel](https://slack-login.timescale.com)
- [Support Email](mailto:support@timescale.com)

### Using TimescaleDB

TimescaleDB scales PostgreSQL for time-series data via automatic
partitioning across time and space (partitioning key), yet retains
the standard PostgreSQL interface.

In other words, TimescaleDB exposes what look like regular tables, but
are actually only an
abstraction (or a virtual view) of many individual tables comprising the
actual data. This single-table view, which we call a
[hypertable](http://docs.timescale.com/introduction/architecture#hypertables),
is comprised of many chunks, which are created by partitioning
the hypertable's data in either one or two dimensions: by a time
interval, and by an (optional) "partition key" such as
device id, location, user id, etc. ([Architecture discussion](http://docs.timescale.com/introduction/architecture))

Virtually all user interactions with TimescaleDB are with
hypertables. Creating tables and indexes, altering tables, inserting
data, selecting data, etc., can (and should) all be executed on the
hypertable.

From the perspective of both use and management, TimescaleDB just
looks and feels like PostgreSQL, and can be managed and queried as
such.


#### Creating a hypertable

```sql
-- We start by creating a regular SQL table
CREATE TABLE conditions (
  time        TIMESTAMPTZ       NOT NULL,
  location    TEXT              NOT NULL,
  temperature DOUBLE PRECISION  NULL,
  humidity    DOUBLE PRECISION  NULL
);

-- Then we convert it into a hypertable that is partitioned by time
SELECT create_hypertable('conditions', 'time');
```

- [Quick start: Creating hypertables](http://docs.timescale.com/getting-started/setup/starting-from-scratch)
- [Reference examples](http://docs.timescale.com/api#schema)

#### Inserting and querying data

Inserting data into the hypertable is done via normal SQL commands:

```sql
INSERT INTO conditions(time, location, temperature, humidity)
  VALUES (NOW(), 'office', 70.0, 50.0);

SELECT * FROM conditions ORDER BY time DESC LIMIT 100;

SELECT time_bucket('15 minutes', time) AS fifteen_min,
    location, COUNT(*),
    MAX(temperature) AS max_temp,
    MAX(humidity) AS max_hum
  FROM conditions
  WHERE time > NOW() - interval '3 hours'
  GROUP BY fifteen_min, location
  ORDER BY fifteen_min DESC, max_temp DESC;
```

In addition, TimescaleDB includes additional functions for time-series
analysis that are not present in vanilla PostgreSQL. (For example, the `time_bucket` function above.)

- [Quick start: Basic operations](http://docs.timescale.com/getting-started/basic-operations)
- [Reference examples](http://docs.timescale.com/api#insert)
- [TimescaleDB API](http://docs.timescale.com/api/api-timescaledb)

### Installation

TimescaleDB can be installed via a variety of ways:

- Linux: [yum](http://docs.timescale.com/getting-started/installation?OS=linux&method=yum), [apt](http://docs.timescale.com/getting-started/installation?OS=linux&method=apt), [Docker](http://docs.timescale.com/getting-started/installation?OS=linux&method=Docker)
- MacOS: [brew](http://docs.timescale.com/getting-started/installation?OS=mac&method=Homebrew), [Docker](http://docs.timescale.com/getting-started/installation?OS=mac&method=Docker)
- Windows: [Docker](http://docs.timescale.com/getting-started/installation?OS=windows&method=Docker)

We recommend following our detailed [installation instructions](http://docs.timescale.com/getting-started/installation).

#### Building from source

**Prerequisites**:  A standard PostgreSQL 9.6 installation with development environment (header files) (e.g., postgresql-server-dev-9.6 package for Linux, Postgres.app for MacOS)

```bash
git clone git@github.com:timescale/timescaledb.git

# To build the extension
make

# To install
make install
```

Please see our [additional configuration instructions](http://docs.timescale.com/getting-started/installation?OS=mac&method=From+Source#update-postgresql-conf-).


### Additional documentation

- [Why use TimescaleDB?](http://docs.timescale.com/introduction)
- [Migrating from PostgreSQL](http://docs.timescale.com/getting-started/setup/migrate-from-postgresql)
- [Writing data](http://docs.timescale.com/api#insert)
- [Querying and data analytics](http://docs.timescale.com/api#select)
- [Tutorials and sample data](http://docs.timescale.com/tutorials)

### Support

- [Github Issues](https://github.com/timescale/timescaledb/issues)
- [Slack Channel](https://slack-login.timescale.com)
- [Support Email](mailto:support@timescale.com)
