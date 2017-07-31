# TimescaleDB Changelog

## 0.3.0 (2017-07-31)

**High-level changes**
* "Upserts" are now supported via normal `ON CONFLICT DO UPDATE`/`ON CONFLICT DO NOTHING` syntax. However, `ON CONFLICT ON CONSTRAINT` is not yet supported.
* Improved support for user-defined triggers on hypertables. Now handles both INSERT BEFORE and INSERT AFTER triggers, and triggers can be named arbitrarily (before, a \_0\_ prefix was required to ensure correct execution priority).
* `TRUNCATE` on a hypertable now deletes empty chunks.

**Notable commits**
* [23f9d3c] Add support for upserts (`ON CONFLICT DO UPDATE`)
* [1f3dcd8] Make `INSERT`s use a custom plan instead of triggers
* [f23bf58] Remove empty chunks on `TRUNCATE` hypertable.

## 0.2.0 (2017-07-12)

**High-level changes**
* Users can now define their own triggers on hypertables (except for `INSERT AFTER`)
* Hypertables can now be renamed or moved to a different schema
* Utility functions added so you can examine the size hypertables, chunks, and indices

**Notable commits**
* [83c75fd] Add support for triggers on hypertables for all triggers except `INSERT AFTER`
* [e0eeeb9] Add hypertable, chunk, and indexes size utils functions.
* [4d2a65d] Add infrastructure to build update script files.
* [a5725d9] Add support to rename and change schema on hypertable.
* [142f58c] Cleanup planner and process utility hooks

## 0.1.0 (2017-06-28)

**IMPORTANT NOTE**

Starting with this release, TimescaleDB will now
support upgrading between extension versions using the typical
`ALTER EXTENSION` command, unless otherwise noted in future release notes. This
important step should make it easier to test TimescaleDB and be able
to get the latest benefits from new versions of TimescaleDB. If you
were previously using a version with the `-beta` tag, you will need
to `DROP` any databases currently using TimescaleDB and re-create them
in order to upgrade to this new version. To backup and migrate data,
use `pg_dump` to save the table schemas and `COPY` to write hypertable
data to CSV for re-importing after upgrading is complete. We describe
a similar process on [our docs](http://docs.timescale.com/getting-started/setup/migrate-from-postgresql#different-db).

**High-level changes**
* More refactoring to stabilize and cleanup the code base for supporting upgrades (see above note)
* Correct handling of ownership and permission propagation for hypertables
* Multiple bug fixes

**Notable commits**
* [696cc4c] Provide API for adding hypertable dimensions
* [97681c2] Fixes permission handling
* [aca7f32] Fix extension drop handling
* [9b8a447] Limit the SubspaceStore size; Add documentation.
* [14ac892] Fix possible segfault
* [0f4169c] Fix check constraint on dimension table
* [71c5e78] Fix and refactor tablespace support
* [5452dc5] Fix partiton functions; bug fixes (including memory)
* [e75cd7e] Finer grained memory management
* [3c460f0] Fix partitioning, memory, and tests
* [fe51d8d] Add native scan for the chunk table
* [fc68baa] Separate out subspace_store and add it to the hypertable object as well
* [c8124b8] Use hypercube instead of dimension slice list
* [f5d7786] Change the semantics of range_end to be exclusive
* [700c9c8] Refactor insert path in C.
* [0584c47] Created chunk_get_or_create in sql with an SPI connector in C
* [7b8de0c] Refactor catalog for new schema and add native data types
* [d3bdcba] Start refactoring to support any number of partitioning dimensions

## 0.0.12-beta (2017-06-21)

**High-level changes**
* A major cleanup and refactoring was done to remove legacy code and
currently unused code paths. This change is **backwards incompatible**
and will require a database to be re-initialized and data re-imported.
This refactoring will allow us to provide upgrade paths starting with
the next release.
* `COPY` and `INSERT` commands now return the correct number of rows
* Default indexes no longer duplicate existing indexes
* Cleanup of the Docker image and build process
* Chunks are now time-aligned across partitions

**Notable commits**
* [3192c8a] Remove Dockerfile and docker.mk
* [2a01ebc] Ensure that chunks are aligned.
* [73622bf] Fix default index creation duplication of indexes
* [c8872fe] Fix command-tag for COPY and INSERT
* [bfe58b6] Refactor towards supporting version upgrades
* [db01c84] Make time-bucket function parallel safe
* [18db11c] Fix timestamp test
* [97bbb59] Make constraint exclusion work with non-text partition keys
* [f2b42eb] Fix problems with partitioning logic for padded fields
* [997029a] if_not_exist flag to create_hypertable now works on hypertables with data as well
* [347a8bd] Reference the correct column when scanning partition epochs
* [88a9849] Fix bug with timescaledb.allow_install_without_preload GUC not working

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
