### Prerequisites

You'll need to install and run [Docker](https://docs.docker.com/engine/installation/),
which is the current way to run the database.

You need to install the [Postgres client](https://wiki.postgresql.org/wiki/Detailed_installation_guides)(psql).


### Building and running in Docker

The `Makefile` included in this repo has convenient commands for building,
starting, and stopping a Docker image of **iobeamdb**:
```bash
# To build the image
make build-docker
# To start the image using docker run.
# This creates a docker container named `iobeamdb`
# with a data volume mount inside the `data` subdirectory.
make docker-run
# To stop the container
docker stop iobeamdb
```

You can then run any other docker command on the container named `iobeamdb`.

With the Docker image running you can run the tests (see Testing) or create
your own single-node cluster.

### Setting up a local single node database
After starting the Docker image, you can start a local single node database:
```bash
make setup-single-node-db
```

This will set up a database named `iobeam` which can be accessed with:
```bash
psql -U postgres -h localhost -d iobeam
```

#### Creating a table
Our specialized time-series table, which is the main abstraction for querying
all your space-time partitions, is called a **hypertable**.

To create a hypertable, you start with a regular SQL table, and then convert it
into a hypertable via the function `create_hypertable()`([API Definition](extension/sql/main/ddl.sql)) (which
  is loaded when you install the extension).

For example, let's create a hypertable for tracking
temperature and humidity across a collection of devices over time.

First, create a regular SQL table:
```sql
CREATE TABLE conditions (
  time TIMESTAMP WITH TIME ZONE NOT NULL,
  device_id TEXT NOT NULL,
  temperature DOUBLE PRECISION NULL,
  humidity DOUBLE PRECISION NULL
);
```

Next, convert it into a hypertable using the provided function
`create_hypertable()`:
```sql
SELECT create_hypertable('"conditions"', 'time', 'device_id');
```

Now, a hypertable that is partitioned on time (using the values in the
`time` column) and on `device_id` has been created.

**Note:**

You can also run the following command from inside the repo to create
the above table for you:
```bash
PGDATABASE=iobeam ./scripts/run_sql.sh setup_sample_hypertable.psql
```

#### Inserting and Querying
Inserting data into the hypertable is done via normal SQL INSERT commands,
e.g. using millisecond timestamps:
```sql
INSERT INTO conditions(time,device_id,temperature,humidity)
VALUES(NOW(), 'office', 70.0, 50.0);
```

Similarly, querying data is done via normal SQL SELECT commands.
SQL UPDATE and DELETE commands also work as expected.

### Indexing data

Data is indexed using normal SQL CREATE INDEX commands. For instance,
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

### Examples
TODO PROVIDE EXAMPLES

### Testing
There are four commands to run tests:
```bash
# Build and run a docker image and run all tests in that image
make test-docker
# Run all tests (no image built)
make test-all
# Run regression tests (no image built)
make test-regression
# Run unit tests (no image built)
make test-unit
```
