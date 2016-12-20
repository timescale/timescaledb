EXTENSION = iobeamdb
SQL_FILES = $(shell cat sql/load_order.txt)

EXT_VERSION = 1.0
EXT_SQL_FILE = iobeamdb--$(EXT_VERSION).sql

DATA = $(EXT_SQL_FILE)
MODULE_big = iobeamdb

SRCS = src/iobeamdb.c src/murmur3.c src/pgmurmur3.c
OBJS = $(SRCS:.c=.o)
EXTENSION = $(MODULE_big)

MKFILE_PATH := $(abspath $(MAKEFILE_LIST))
CURRENT_DIR = $(dir $(MKFILE_PATH))

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

override CFLAGS += -DINCLUDE_PACKAGE_SUPPORT=0

check-sql-files:
	@echo $(EXT_SQL_FILES)

generate-sql-install-file: 
	@cat $(SQL_FILES) > $(EXT_SQL_FILE)

install: generate-sql-install-file

package: clean all generate-sql-install-file
	@mkdir -p package/lib
	@mkdir -p package/extension
	$(install_sh) -m 755 iobeamdb.so 'package/lib/iobeamdb.so'
	$(install_sh) -m 644 iobeamdb.control 'package/extension/'
	$(install_sh) -m 644 iobeamdb--1.0.sql  'package/extension/'

.PHONY: check-sql-files generate-sql-install-file start-pg-docker stop-pg-docker test test-regression test-unit
