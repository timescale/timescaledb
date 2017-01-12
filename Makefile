
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

start-docker: stop-docker
	@IOBEAMDB_DOCKER_IMAGE=$(IMAGE_NAME) ./scripts/start-docker.sh

stop-docker:
	@docker rm -f iobeamdb || :

# Targets for tests
test-regression:
	@cd extension/sql/tests/regression; ./run.sh

test-unit:
	@cd extension/sql/tests/unit; ./run.sh

test-all: test-regression test-unit
	@echo Running all tests

test-docker: build-docker start-docker test-all stop-docker

# Setting up a single node database
setup-single-node-db:
	PGDATABASE=test ./scripts/run_sql.sh setup_single_node_db.psql

.PHONY: build-docker start-docker stop-docker test-regression test-unit test-all test all setup-single-node-db
