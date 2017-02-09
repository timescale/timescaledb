## Prerequisites

- The [Postgres client](https://wiki.postgresql.org/wiki/Detailed_installation_guides) (psql)
- A standard PostgreSQL installation with development environment (header files), or
- Docker (see separate build and run instructions)

## Installation

### Option 1: Build and install with local PostgreSQL

```bash
# To build the extension
make

# To install
make install

# To run tests (needs running Postgres server with preloaded extension)
make installcheck
```

### Option 2: Build and run in Docker

```bash
# To build a Docker image
make -f docker.mk build-image

# To run a container
make -f docker.mk run

# To run tests
make -f docker.mk test
```

### Setting up your database

When creating a new database, it is necessary to install the extension and
run an initialization function.

```sql
# Install the extension
CREATE database test;
\c test
CREATE EXTENSION IF NOT EXISTS iobeamdb CASCADE;
select setup_single_node();
```

For convenience, this can also be done in one step by running a script from
the command-line:
```bash
DB_NAME=iobeamdb ./scripts/setup-db.sh
```

## Working with time-series data

This extension allows creating time-series optimized data tables,
called **hypertables**.

### Creating (hyper)tables
To create a hypertable, you start with a regular SQL table, and then convert it
into a hypertable via the function `create_hypertable()`([API Definition](extension/sql/main/ddl.sql)).

The following example creates a hypertable for tracking
temperature and humidity across a collection of devices over time.

```sql
CREATE TABLE conditions (
  time TIMESTAMP WITH TIME ZONE NOT NULL,
  device_id TEXT NOT NULL,
  temperature DOUBLE PRECISION NULL,
  humidity DOUBLE PRECISION NULL
);
```

Next, make it a hypertable using the provided function
`create_hypertable()`:
```sql
SELECT create_hypertable('"conditions"', 'time', 'device_id');
```

The hypertable will partition its data along two dimensions: time (using the values in the
`time` column) and space on `device_id`. Note, however, that space partitioning
is optional, so one can also do:
```sql
SELECT create_hypertable('"conditions"', 'time');
```

### Inserting and Querying
Inserting data into the hypertable is done via normal SQL `INSERT` commands,
e.g. using millisecond timestamps:
```sql
INSERT INTO conditions(time,device_id,temperature,humidity)
VALUES(NOW(), 'office', 70.0, 50.0);
```

Similarly, querying data is done via normal SQL `SELECT` commands.
SQL `UPDATE` and `DELETE` commands also work as expected.

### Indexing data

Data is indexed using normal SQL `CREATE INDEX` commands. For instance,
```sql
CREATE INDEX ON conditions (device_id, time DESC);
```
This can be done before or after converting the table to a hypertable.

**Indexing suggestions:**

Our experience has shown that different types of indexes are most-useful for
time-series data, depending on your data.

For indexing columns with discrete (limited-cardinality) values (e.g., where you are most likely
  to use an "equals" or "not equals" comparator) we suggest using an index like this (using our hypertable `conditions` for the example):
```sql
CREATE INDEX ON conditions (device_id, time DESC);
```
For all other types of columns, i.e., columns with continuous values (e.g., where you are most likely to use a
"less than" or "greater than" comparator) the index should be in the form:
```sql
CREATE INDEX ON conditions (time DESC, temperature);
```
Having a `time DESC` column specification in the index allows for efficient queries by column-value and time. For example, the index defined above would optimize the following query:
```sql
SELECT * FROM conditions WHERE device_id = 'dev_1' ORDER BY time DESC LIMIT 10
```

For sparse data where a column is often NULL, we suggest adding a `WHERE column IS NOT NULL` clause to the index (unless you are often searching for missing data). For example,

```sql
CREATE INDEX ON conditions (time DESC, humidity) WHERE humidity IS NOT NULL;
```
this creates a more compact, and thus efficient, index.

### Dropping old data using a retention policy

Hypertables allow dropping old data at almost no cost using a built-in API that
makes it easy to implement retention policies. To drop data older than three months
from the table `temperature_data`:
```sql
SELECT drop_chunks(interval '3 months', 'temperature_data');
```

This works at the level of table chunks, so if a chunk covers 24 hours of data
the table might retain up to that amount of additional data (above the three months).
One can also drop chunks from all hypertables in the database by simply doing:
```sql
SELECT drop_chunks(interval '3 months');
```

For automatic data retention, the above calls can be added to, e.g., a CRON job
on the database host.

## Sample datasets
To help you quickly get started, we have created some sample datasets. See
[Using our samples](docs/UsingSamples.md) for further instructions.