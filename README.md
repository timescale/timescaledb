[![Build Status](https://travis-ci.org/timescale/timescaledb.svg?branch=master)](https://travis-ci.org/timescale/timescaledb)

TimescaleDB is an open-source database designed to make SQL scalable for
time-series data. It is engineered up from PostgreSQL, providing automatic
partitioning across time and space (partitioning key), as well as full
SQL support.

TimescaleDB is packaged as a PostgreSQL extension and set of scripts.

For a more detailed description of our architecture, [please read
the technical
paper](http://www.timescaledb.com/papers/timescaledb.pdf). Additionally,
more documentation can be
found [on our docs website](https://docs.timescaledb.com/).

There are several ways to install TimescaleDB: (1) Homebrew (for MacOS),
(2) Docker, or (3) from source.

## Installation

_NOTE: Currently, upgrading to new versions requires a fresh install._

**Prerequisite**

- The [Postgres client][Postgres-client] (psql) is required for all of the following installation methods.

[Postgres-client]: https://wiki.postgresql.org/wiki/Detailed_installation_guides

### Option 1 - Homebrew

This will install PostgreSQL 9.6 via Homebrew as well. If you have
another installation (such as Postgres.app), this will cause problems. We
recommend removing other installations before using this method.

**Prerequisites**

- [Homebrew](https://brew.sh/)

**Build and install**

```bash
# Add our tap
brew tap timescale/tap

# To install
brew install timescaledb
```

**Update `postgresql.conf`**

Also, you will need to edit your `postgresql.conf` file to include
necessary libraries:
```bash
# Modify postgresql.conf to uncomment this line and add required libraries.
# For example:
shared_preload_libraries = 'timescaledb'
```

To get started you'll now need to restart PostgreSQL and add a
`postgres` superuser (used in the rest of the docs):
```bash
# Restart PostgreSQL
brew services restart postgresql

# Add a superuser postgres:
createuser postgres -s
```

### Option 2 - Docker Hub

You can pull our Docker images from [Docker Hub](https://hub.docker.com/r/timescale/timescaledb/).

```bash
docker pull timescale/timescaledb:latest
```

To run, you'll need to specify a directory where data should be
stored/mounted from on the host machine. For example, if you want
to store the data in `/your/data/dir` on the host machine:
```bash
docker run -d \
  --name timescaledb \
  -v /your/data/dir:/var/lib/postgresql/data \
  -p 5432:5432 \
  -e PGDATA=/var/lib/postgresql/data/timescaledb \
  timescale/timescaledb postgres \
  -cshared_preload_libraries=timescaledb
```
In particular, the `-v` flag sets where the data is stored. If not set,
the data will be dropped when the container is stopped.

You can write the above command to a shell script for easy use, or use
our [docker-run.sh](scripts/docker-run.sh) in `scripts/`, which saves
the data to `$PWD/data`. There you can also see additional `-c` flags
we recommend for memory settings, etc.

### Option 3 - From source
We have only tested our build process on **MacOS and Linux**. We do
not support building on Windows yet. Windows may be able to use our
Docker image on Docker Hub (see above).

**Prerequisites**

- A standard **PostgreSQL 9.6** installation with development environment (header files) (e.g., [Postgres.app for MacOS](https://postgresapp.com/))

**Build and install with local PostgreSQL**

```bash
# To build the extension
make

# To install
make install
```

**Update `postgresql.conf`**

Also, you will need to edit your `postgresql.conf` file to include
necessary libraries, and then restart PostgreSQL:
```bash
# Modify postgresql.conf to uncomment this line and add required libraries.
# For example:
shared_preload_libraries = 'timescaledb'

# Then, restart PostgreSQL
```


### Setting up your initial database
Now, we'll install our extension and create an initial database. Below
you'll find instructions for creating a new, empty database.

To help you quickly get started, we have also created some sample
datasets. Once you complete the initial setup below you can then
easily import this data to play around with TimescaleDB functionality.
See [our Sample Datasets](https://docs.timescale.com/other-sample-datasets)
for further instructions.

#### Setting up an empty database

When creating a new database, it is necessary to install the extension and then run an initialization function.

```bash
# Connect to Postgres, using a superuser named 'postgres'
psql -U postgres -h localhost
```

```sql
-- Install the extension
CREATE database tutorial;
\c tutorial
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
```

For convenience, this can also be done in one step by running a script from
the command-line:
```bash
DB_NAME=tutorial ./scripts/setup-db.sh
```

#### Accessing your new database
You should now have a brand new time-series database running in Postgres.

```bash
# To access your new database
psql -U postgres -h localhost -d tutorial
```

Next let's load some data.

## Working with time-series data

One of the core ideas of our time-series database are time-series optimized data tables, called **hypertables**.

### Creating a (hyper)table
To create a hypertable, you start with a regular SQL table, and then convert
it into a hypertable via the function
`create_hypertable()`([API definition](docs/API.md)).

The following example creates a hypertable for tracking
temperature and humidity across a collection of devices over time.

```sql
-- We start by creating a regular SQL table
CREATE TABLE conditions (
  time        TIMESTAMPTZ       NOT NULL,
  location    TEXT              NOT NULL,
  temperature DOUBLE PRECISION  NULL,
  humidity    DOUBLE PRECISION  NULL
);
```

Next, transform it into a hypertable using the provided function
`create_hypertable()`:

```sql
-- This creates a hypertable that is partitioned by time
--   using the values in the `time` column.
SELECT create_hypertable('conditions', 'time');

-- OR you can additionally partition the data on another dimension
--   (what we call 'space') such as `location`.
-- For example, to partition `location` into 2 partitions:
SELECT create_hypertable('conditions', 'time', 'location', 2);
```

### Inserting and querying
Inserting data into the hypertable is done via normal SQL `INSERT` commands,
e.g. using millisecond timestamps:
```sql
INSERT INTO conditions(time, location, temperature, humidity)
VALUES(NOW(), 'office', 70.0, 50.0);
```

Similarly, querying data is done via normal SQL `SELECT` commands.
SQL `UPDATE` and `DELETE` commands also work as expected.

### Indexing data

Data is indexed using normal SQL `CREATE INDEX` commands. For instance,
```sql
CREATE INDEX ON conditions (location, time DESC);
```
This can be done before or after converting the table to a hypertable.

**Indexing suggestions:**

Our experience has shown that different types of indexes are most-useful for
time-series data, depending on your data.

For indexing columns with discrete (limited-cardinality) values (e.g., where you are most likely
  to use an "equals" or "not equals" comparator) we suggest using an index like this (using our hypertable `conditions` for the example):
```sql
CREATE INDEX ON conditions (location, time DESC);
```
For all other types of columns, i.e., columns with continuous values (e.g., where you are most likely to use a
"less than" or "greater than" comparator) the index should be in the form:
```sql
CREATE INDEX ON conditions (time DESC, temperature);
```
Having a `time DESC` column specification in the index allows for efficient queries by column-value and time. For example, the index defined above would optimize the following query:
```sql
SELECT * FROM conditions WHERE location = 'garage' ORDER BY time DESC LIMIT 10
```

For sparse data where a column is often NULL, we suggest adding a
`WHERE column IS NOT NULL` clause to the index (unless you are often
searching for missing data). For example,

```sql
CREATE INDEX ON conditions (time DESC, humidity) WHERE humidity IS NOT NULL;
```
this creates a more compact, and thus efficient, index.

### Current limitations
Below are a few current limitations of our database, which we are
actively working to resolve:

- Any user has full read/write access to the metadata tables for hypertables.
- Permission changes on hypertables are not correctly propagated.
- `create_hypertable()` can only be run on an empty table
- Custom user-created triggers on hypertables currently not allowed
- `drop_chunks()` (see our [API Reference](docs/API.md)) is currently only
supported for hypertables that are not partitioned by space.

### Restoring a database from backup.

A database with the timescaledb extension can be backed up using normal backup
procedures (e.g. `pg\_dump`). However, when restoring the database the following
procedure must be used.

```sql
CREATE DATABASE db_for_restore;
ALTER DATABASE db_for_restore SET timescaledb.restoring='on';

--execute the restore below:
\! pg_restore -h localhost -U postgres -d single dump/single.sql

--connect to the restored db;
\c db_for_restore
SELECT restore_timescaledb();
ALTER DATABASE single SET timescaledb.restoring='off';
```

Note: You must use pg_dump and pg_restore versions 9.6.2 and above.

### More APIs
For more information on TimescaleDB's APIs, check out our
[API Reference](docs/API.md).

## Testing
If you want to contribute, please make sure to run the test suite before
submitting a PR.

If you are running locally:
```bash
make installcheck
```

If you are using Docker:
```bash
make -f docker.mk test
```
