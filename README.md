### Running docker image

To start the database, simply:
```bash
make start-pg-docker
```
This will allow you to run the tests. When finished,
```bash
make stop-pg-docker
```

### Testing
There are three commands to run tests: (1) all tests, (2) regression tests, and
(3) unit tests. They are:
```bash
make test
make test-regression
make test-unit
```
