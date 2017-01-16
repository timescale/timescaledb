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

#### Initializing a single-node database
After starting the Docker image, you can start a local single node database:
```bash
make setup-single-node-db
```

This will set up a database named `iobeam` which can be accessed with:
```bash
psql -U postgres -h localhost -d iobeam
```

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
