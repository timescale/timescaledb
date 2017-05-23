# TimescaleDB Changelog

## 0.0.11-beta (2017-05-24)

**High-level changes**
* New `first(value, time)` and `last(value, time)` aggregates
* Remove `setup_timescaledb()` function to streamline setup
* Allow for use cases where restarting the server is not feasible by force loading the library
* Disable time series optimizations on non-hypertables
* Add some default indexes for hypertables if they do not exist
* Add "if not exists" flag for `create_hypertable`
* Several bug fixes and cleanups

**Notable commits**
* [8ccc8cc] Add if_not_exists flag to create_hypertable()
* [2bc60c7] Fix time interval field name in hypertable cache entry
* [4638688] Improve GUC handling
* [cedcafc] Remove setup_timescaledb() and fix pg_dump/pg_restore.
* [34ad9a0] Add error when timescaledb library is not preloaded.
* [fc4ddd6] Fix bug with dropping chunks on tables with indexes
* [32215ff] Add default indexes for hypertables
* [b2900f9] Disable query optimization on regular tables (non-hypertables)
* [f227db4] Fixes command tag return for COPY on hypertables.
* [eb32081] Fix Invalid database ID error
* [662be94] Add the first(value, time),last(value, time) aggregates
* [384a8fb] Add regression tests for deleted unit tests
* [31ee92a] Remove unit tests and sql/setup
* [13d3acb] Fix bug with alter table add/drop column if exists
* [f960c24] Fix bug with querying a row as a composite type

## 0.0.10-beta (2017-05-04)

**High-level changes**
* New `time_bucket` functions for doing roll-ups on varied intervals
* Change default partition function (thanks @robin900)
* Variety of bug fixes

**Notable commits**
* [1c4868d] Add documentation for chunk_time_interval argument
* [55fd2f2] Fixes command tag return for `INSERT`s on hypertables.
* [c3f930f] Add `time_bucket` functions
* [b128ac2] Fix bug with `INSERT INTO...SELECT`
* [e20edf8] Add better error checking for index creation.
* [72f754a] use PostgreSQL's own `hash_any` function as default partfunc (thanks @robin900)
* [39f4c0f] Remove sample data instructions and point to docs site
* [9015314] Revised the `get_general_index_definition` function to handle cases where indexes have definitions other than just `CREATE INDEX` (thanks @bricklen)
