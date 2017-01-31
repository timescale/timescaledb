
IMAGE_NAME = iobeamdb
MAKE = make

all: test-docker

# Targets for installing the extension without using Docker
clean:
	$(MAKE) -C ./extension clean
	@rm -f ./extension/iobeamdb--*.sql

install:
	$(MAKE) -C ./extension install

# Targets for building/running Docker images
build-docker:
	@docker build . -t $(IMAGE_NAME)

docker-run:
	@IMAGE_NAME=$(IMAGE_NAME) ./scripts/docker-run.sh

start-test-docker:
	@ . scripts/test-docker-config.sh && IMAGE_NAME=$(IMAGE_NAME) CONTAINER_NAME=iobeamdb_testing ./scripts/start-test-docker.sh

stop-test-docker: 
	@docker rm -f iobeamdb_testing

# Targets for tests
test-regression:
	@ . scripts/test-docker-config.sh && cd extension/sql/tests/regression && ./run.sh

test-unit:
	@ . scripts/test-docker-config.sh && cd extension/sql/tests/unit && ./run.sh

test-all: test-regression test-unit
	@echo Running all tests

test-docker: build-docker start-test-docker test-all stop-test-docker

# Setting up a single node database
setup-single-node-db:
	@PGDATABASE=postgres ./scripts/run_sql.sh setup_single_node_db.psql

.PHONY: build-docker start-docker stop-docker test-regression test-unit test-all test all setup-single-node-db
