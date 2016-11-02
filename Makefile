start-pg-docker:
	@./start-pg-docker.sh

stop-pg-docker:
	@docker rm -f postgres || :

test-regression:
	@cd sql/tests/regression; ./run.sh

test-unit:
	@cd sql/tests/unit; ./run.sh

test: test-regression test-unit

.PHONY: start-pg-docker stop-pg-docker test test-regression test-unit
