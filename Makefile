EXTENSION = timescaledb
SQL_FILES = $(shell cat sql/load_order.txt)

EXT_VERSION = 0.0.1-beta
EXT_SQL_FILE = sql/$(EXTENSION)--$(EXT_VERSION).sql

DATA = $(EXT_SQL_FILE)
MODULE_big = $(EXTENSION)

SRCS = \
	src/timescaledb.c \
	src/murmur3.c \
	src/pgmurmur3.c \
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
	src/insert.c

OBJS = $(SRCS:.c=.o)

MKFILE_PATH := $(abspath $(MAKEFILE_LIST))
CURRENT_DIR = $(dir $(MKFILE_PATH))

TEST_PGPORT ?= 5432
TEST_PGHOST ?= localhost
TEST_PGUSER ?= postgres
TESTS = $(sort $(wildcard test/sql/*.sql))
USE_MODULE_DB=true
REGRESS = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = \
	--inputdir=test \
	--outputdir=test \
	--launcher=test/runner.sh \
	--host=$(TEST_PGHOST) \
	--port=$(TEST_PGPORT) \
	--user=$(TEST_PGUSER) \
	--load-language=plpgsql \
	--load-extension=dblink \
	--load-extension=postgres_fdw \
	--load-extension=hstore \
	--load-extension=timescaledb

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

EXTRA_CLEAN = $(EXT_SQL_FILE)

include $(PGXS)
override CFLAGS += -DINCLUDE_PACKAGE_SUPPORT=0
override pg_regress_clean_files = test/results/ test/regression.diffs test/regression.out tmp_check/ log/

all: $(EXT_SQL_FILE)

$(EXT_SQL_FILE): $(SQL_FILES)
	@cat $^ > $@

check-sql-files:
	@echo $(SQL_FILES)

install: $(EXT_SQL_FILE)

package: clean $(EXT_SQL_FILE)
	@mkdir -p package/lib
	@mkdir -p package/extension
	$(install_sh) -m 755 $(EXTENSION).so 'package/lib/$(EXTENSION).so'
	$(install_sh) -m 644 $(EXTENSION).control 'package/extension/'
	$(install_sh) -m 644 $(EXT_SQL_FILE)  'package/extension/'

typedef.list: clean $(OBJS)
	./generate_typedef.sh

pgindent: typedef.list
	pgindent --typedef=typedef.list


.PHONY: check-sql-files all
