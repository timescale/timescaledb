### Running docker image

To start the database, simply:
```bash
make start-pg-docker
```
This will allow you to run the tests. When finished,
```bash
make stop-pg-docker
```

### Initializing a single-node database
After setting up a docker image, you can start a local 
single node database with the following command:
```bash
make setup-single-node-db
```
This will set up a database named iobeam which can be accessed with:
```bash
psql -U postgres -h localhost -d iobeam
```

### Examples

 * [DDL Operations](extension/sql/tests/regression/ddl.sql)
 * [Insert Operations](extension/sql/tests/regression/insert.sql)
 * [Querying With Ioql](extension/sql/tests/regression/query.sql)

### Testing
There are three commands to run tests: (1) all tests, (2) regression tests, and
(3) unit tests. They are:
```bash
make test
make test-regression
make test-unit
```
