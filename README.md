### Building and running in Docker

The `Makefile` included in this repo has convenient commands for building,
starting, and stopping a Docker image of **iobeamdb**:
```bash
# To build the image
make build-docker
# To start the image (will stop any running instances first)
make start-docker
# To stop the image
make stop-docker
```

With the Docker image running you can run the tests (see Testing) or create
your own single-node cluster.

### Getting started
After starting the Docker image, you can start a local single node database:
```bash
make setup-single-node-db
```

This will set up a database named `iobeam` which can be accessed with:
```bash
psql -U postgres -h localhost -d iobeam
```

#### Creating a hypertable
To create our specialized time-series table, called a **hypertable**, you
start with a regular SQL table. For example, here's one for tracking
temperature and humidity from a collection of devices over time:
```sql
CREATE TABLE conditions (
  time BIGINT NOT NULL,
  device_id TEXT NOT NULL,
  temp DOUBLE PRECISION NULL,
  humidity DOUBLE PRECISION NULL
);
```

This can be turned into a hypertable using the provided function
`create_hypertable()` that is loaded when you initialize the cluster:
```sql
SELECT name FROM create_hypertable('"conditions"', 'time', 'device_id');
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
INSERT INTO conditions(time,device_id,temp,humidity)
VALUES(1484850291000, 'office', 70.0, 50.0);
```

Similarly, querying data is done via normal SQL SELECT commands. Updating
and deleting individual rows is currently _not_ supported.

### Examples

 * [DDL Operations](extension/sql/tests/regression/ddl.sql)
 * [Insert Operations](extension/sql/tests/regression/insert.sql)
 * [Querying With Ioql](extension/sql/tests/regression/query.sql)

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
