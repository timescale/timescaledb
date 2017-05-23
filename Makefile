PG_CONFIG = pg_config
MIN_SUPPORTED_VERSION = $(shell cat src/init.c | grep -m 1 'MIN_SUPPORTED_VERSION_STR' | sed "s/^\#define MIN_SUPPORTED_VERSION_STR \"\([0-9]*\.[0-9]*\)\"$\/\1/g")

# will need to change when we support more than one version
ifneq ($(MIN_SUPPORTED_VERSION), $(shell $(PG_CONFIG) --version | sed "s/^PostgreSQL \([0-9]*\.[0-9]*\)\.[0-9]*$\/\1/g"))
$(error "TimescaleDB requires PostgreSQL $(MIN_SUPPORTED_VERSION)")
endif

EXTENSION = timescaledb
SQL_FILES = $(shell cat sql/load_order.txt)

EXT_VERSION = $(shell cat timescaledb.control | grep 'default' | sed "s/^.*'\(.*\)'$\/\1/g")
EXT_SQL_FILE = sql/$(EXTENSION)--$(EXT_VERSION).sql

DATA = $(EXT_SQL_FILE)
MODULE_big = $(EXTENSION)

SRCS = \
	src/init.c \
	src/extension.c \
	src/utils.c \
	src/catalog.c \
	src/metadata_queries.c \
	src/cache.c \
	src/cache_invalidate.c \
	src/chunk.c \
	src/scanner.c \
	src/hypertable_cache.c \
	src/chunk_cache.c \
	src/partitioning.c \
	src/insert.c \
	src/planner.c \
	src/executor.c \
	src/process_utility.c \
	src/sort_transform.c \
	src/insert_chunk_state.c \
	src/insert_statement_state.c \
	src/agg_bookend.c \
	src/guc.c

OBJS = $(SRCS:.c=.o)
DEPS = $(SRCS:.c=.d)

MKFILE_PATH := $(abspath $(MAKEFILE_LIST))
CURRENT_DIR = $(dir $(MKFILE_PATH))

TEST_PGPORT ?= 15432
TEST_PGHOST ?= localhost
TEST_PGUSER ?= postgres
TEST_DIR = test
TEST_CLUSTER ?= $(TEST_DIR)/testcluster
TEST_INSTANCE_OPTS ?= \
	--create-role=$(TEST_PGUSER) \
	--temp-instance=$(TEST_CLUSTER) \
	--temp-config=$(TEST_DIR)/postgresql.conf

EXTRA_REGRESS_OPTS ?=

export TEST_PGUSER

TESTS = $(sort $(wildcard test/sql/*.sql))
USE_MODULE_DB=true
REGRESS = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = \
	$(TEST_INSTANCE_OPTS) \
	--inputdir=$(TEST_DIR) \
	--outputdir=$(TEST_DIR) \
	--launcher=$(TEST_DIR)/runner.sh \
	--host=$(TEST_PGHOST) \
	--port=$(TEST_PGPORT) \
	--user=$(TEST_PGUSER) \
	--load-language=plpgsql \
	--load-extension=$(EXTENSION) \
	$(EXTRA_REGRESS_OPTS)

PGXS := $(shell $(PG_CONFIG) --pgxs)

EXTRA_CLEAN = $(EXT_SQL_FILE) $(DEPS)

include $(PGXS)
override CFLAGS += -DINCLUDE_PACKAGE_SUPPORT=0 -MMD
override pg_regress_clean_files = test/results/ test/regression.diffs test/regression.out tmp_check/ log/ $(TEST_CLUSTER)
-include $(DEPS)

all: $(EXT_SQL_FILE)

$(EXT_SQL_FILE): $(SQL_FILES)
	@cat $^ > $@

check-sql-files:
	@echo $(SQL_FILES)

install: $(EXT_SQL_FILE)

clean: clean-sql-files

clean-sql-files:
	@rm -f sql/$(EXTENSION)--*.sql

package: clean $(EXT_SQL_FILE)
	@mkdir -p package/lib
	@mkdir -p package/extension
	$(install_sh) -m 755 $(EXTENSION).so 'package/lib/$(EXTENSION).so'
	$(install_sh) -m 644 $(EXTENSION).control 'package/extension/'
	$(install_sh) -m 644 $(EXT_SQL_FILE) 'package/extension/'

typedef.list: clean $(OBJS)
	./scripts/generate_typedef.sh

pgindent: typedef.list
	pgindent --typedef=typedef.list

.PHONY: check-sql-files all
