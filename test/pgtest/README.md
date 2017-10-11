# PostgreSQL tests for TimescaleDB

The CMake configuration within this directory makes it possible to run
the standard PostgreSQL test suite with the TimescaleDB extension
loaded. This is useful to ensure that TimescaleDBs modifications
planner and DDL hooks are compatible with standard PostgreSQL.

## Running

The configuration within adds a new CMake target, `pginstallcheck`,
that allows running the PostgreSQL test suite using a modified test
schedule. The target requires access to the PostgreSQL source code,
which can be configured via the `PG_SOURCE_DIR` CMake variable. The
source tree needs to be compiled, at least the `src/test/regress`
directory. If the path to a PostgreSQL source tree is not
auto-detected, this variable can be set manually to point to the right
location.

```
# In top-level directory of a TimescaleDB source tree
$ mkdir build && cd build
$ cmake -DPG_SOURCE_DIR=<path/to/pg/source> ..
```

Once CMake is correctly configured, run:

```
$ make pginstallcheck
```
