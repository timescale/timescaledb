|Linux/macOS|Windows|Coverity|Code Coverage|
|:---:|:---:|:---:|:---:|
|[![Build Status](https://travis-ci.org/timescale/timescaledb.svg?branch=master)](https://travis-ci.org/timescale/timescaledb)|[![Windows build status](https://ci.appveyor.com/api/projects/status/15sqkl900t04hywu/branch/master?svg=true)](https://ci.appveyor.com/project/RobAtticus/timescaledb/branch/master)|[![Coverity Scan Build Status](https://scan.coverity.com/projects/timescale-timescaledb/badge.svg)](https://scan.coverity.com/projects/timescale-timescaledb)|[![Code Coverage](https://codecov.io/gh/timescale/timescaledb/branch/master/graphs/badge.svg?branch=master)](https://codecov.io/gh/timescale/timescaledb)


## TimescaleDB

TimescaleDB is an open-source database designed to make SQL scalable for
time-series data. It is engineered up from PostgreSQL, providing automatic
partitioning across time and space (partitioning key), as well as full
SQL support.

TimescaleDB is packaged as a PostgreSQL extension and released under
the Apache 2 open-source license. [Contributors welcome.](https://github.com/timescale/timescaledb/blob/master/CONTRIBUTING.md)

Below is an introduction to TimescaleDB. For more information, please check out these other resources:
- [Developer Documentation](https://docs.timescale.com/)
- [Slack Channel](https://slack-login.timescale.com)
- [Support Email](mailto:support@timescale.com)

(Before building from source, see instructions below.)

### Using TimescaleDB

TimescaleDB scales PostgreSQL for time-series data via automatic
partitioning across time and space (partitioning key), yet retains
the standard PostgreSQL interface.

In other words, TimescaleDB exposes what look like regular tables, but
are actually only an
abstraction (or a virtual view) of many individual tables comprising the
actual data. This single-table view, which we call a
[hypertable](https://docs.timescale.com/latest/introduction/architecture#hypertables),
is comprised of many chunks, which are created by partitioning
the hypertable's data in either one or two dimensions: by a time
interval, and by an (optional) "partition key" such as
device id, location, user id, etc. ([Architecture discussion](https://docs.timescale.com/latest/introduction/architecture))

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

- [Quick start: Creating hypertables](https://docs.timescale.com/latest/getting-started/creating-hypertables)
- [Reference examples](https://docs.timescale.com/latest/using-timescaledb/schema-management)

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

- [Quick start: Basic operations](https://docs.timescale.com/latest/getting-started/basic-operations)
- [Reference examples](https://docs.timescale.com/latest/using-timescaledb/writing-data)
- [TimescaleDB API](https://docs.timescale.com/latest/api)

### Installation

TimescaleDB can be installed via a variety of ways:

- Linux: [yum](https://docs.timescale.com/latest/getting-started/installation/linux/installation-yum), [apt (Ubuntu)](https://docs.timescale.com/latest/getting-started/installation/linux/installation-apt-ubuntu), [apt (Debian)](https://docs.timescale.com/latest/getting-started/installation/linux/installation-apt-debian), [Docker](https://docs.timescale.com/latest/getting-started/installation/linux/installation-docker)
- MacOS: [brew](https://docs.timescale.com/latest/getting-started/installation/mac/installation-homebrew), [Docker](https://docs.timescale.com/latest/getting-started/installation/mac/installation-docker)
- Windows: [Docker](https://docs.timescale.com/latest/getting-started/installation/windows/installation-docker)

We recommend following our detailed [installation instructions](https://docs.timescale.com/latest/getting-started/installation).

#### Building from source (Unix-based systems)

If you are building from source for **non-development purposes**
(i.e., you want to run TimescaleDB, not submit a patch), you should
**always use a release-tagged commit and not build from `master`**.
See the Releases tab for the latest release.

**Prerequisites**:

- A standard PostgreSQL 9.6 or 10 installation with development
environment (header files) (e.g., `postgresql-server-dev-9.6 `package
for Linux, Postgres.app for MacOS)
- C compiler (e.g., gcc or clang)
- [CMake](https://cmake.org/) version 3.4 or greater

```bash
git clone git@github.com:timescale/timescaledb.git
cd timescaledb

# Find the latest release and checkout, e.g. for 0.8.0:
git checkout 0.8.0

# Bootstrap the build system
./bootstrap

# To build the extension
cd build && make

# To install
make install
```

Please see our [additional configuration instructions](https://docs.timescale.com/latest/getting-started/installation#update-postgresql-conf).

#### Building from source (Windows)

If you are building from source for **non-development purposes**
(i.e., you want to run TimescaleDB, not submit a patch), you should
**always use a release-tagged commit and not build from `master`**.
See the Releases tab for the latest release.

**Prerequisites**:

- A standard [PostgreSQL 9.6 or 10 64-bit installation](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads#windows)
- Microsoft Visual Studio 2017 with CMake and Git components
- OR Visual Studio 2015/2016 with [CMake](https://cmake.org/) version 3.4 or greater and Git
- Make sure all relevant binaries are in your PATH: `pg_config`, `cmake`, `MSBuild`

If using Visual Studio 2017 with the CMake and Git components, you
should be able to simply clone the repo and open the folder in
Visual Studio which will take care of the rest.

If you are using an earlier version of Visual Studio, then it can
be built in the following way:
```bash
git clone git@github.com:timescale/timescaledb.git
cd timescaledb

# Find the latest release and checkout, e.g. for 0.8.0:
git checkout 0.8.0

# Bootstrap the build system
./bootstrap.bat

# To build the extension from command line
cd build
MSBuild.exe timescaledb.sln

# To install
MSBuild.exe /p:Configuration=Release INSTALL.vcxproj

# Alternatively, open build/timescaledb.sln in Visual Studio and build
```

### Additional documentation

- [Why use TimescaleDB?](https://docs.timescale.com/latest/introduction)
- [Migrating from PostgreSQL](https://docs.timescale.com/latest/getting-started/setup/migrate-from-postgresql)
- [Writing data](https://docs.timescale.com/latest/using-timescaledb/writing-data)
- [Querying and data analytics](https://docs.timescale.com/latest/using-timescaledb/reading-data)
- [Tutorials and sample data](https://docs.timescale.com/latest/tutorials)

### Support

- [Slack Channel](https://slack.timescale.com)
- [Github Issues](https://github.com/timescale/timescaledb/issues)
- [Support Email](mailto:support@timescale.com)

### Contributing

- [Contributor instructions](https://github.com/timescale/timescaledb/blob/master/CONTRIBUTING.md)
- [Code style guide](https://github.com/timescale/timescaledb/blob/master/docs/StyleGuide.md)
