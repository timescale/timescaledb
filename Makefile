
TEST_IMAGE_NAME = iobeamdb-test
TEST_CONTAINER_NAME = iobeamdb-test-container

MAKE = make

all: test 

build-test-docker: 
	@docker build . -t $(TEST_IMAGE_NAME)

start-test-docker: stop-test-docker
	@IOBEAMDB_DOCKER_IMAGE=$(TEST_IMAGE_NAME) ./scripts/start-pg-docker.sh

stop-test-docker:
	@docker rm -f iobeamdb || :

test-regression:
	@cd extension/sql/tests/regression; ./run.sh

test-unit:
	@cd extension/sql/tests/unit; ./run.sh

test-all: test-regression test-unit
	@echo Running all tests

setup-single-node-db:
	PGDATABASE=test ./scripts/run_sql.sh setup_single_node_db.psql 

test: build-test-docker start-test-docker test-all stop-test-docker

.PHONY: build-test-docker start-test-docker stop-test-docker test-regression test-unit test-all test all setup-single-node-db
