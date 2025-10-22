# TimescaleDB Changelog

**Please note: When updating your database, you should connect using
This page lists all the latest features and updates to TimescaleDB. When 
you use psql to update your database, use the -X flag and prevent any .psqlrc 
commands from accidentally triggering the load of a previous DB version.**

## 2.23.0 (2025-10-22)

This release contains performance improvements and bug fixes since the 2.22.1 release. We recommend that you upgrade at the next available opportunity.

**Highlighted features in TimescaleDB v2.23.0**
* 

**Features**
* [#8373](https://github.com/timescale/timescaledb/pull/8373) More precise estimates of row numbers for columnar storage based on Postgres statistics
* [#8581](https://github.com/timescale/timescaledb/pull/8581) Allow mixing postgres and timescaledb options in ALTER TABLE SET
* [#8582](https://github.com/timescale/timescaledb/pull/8582) Make partition_column in CREATE TABLE WITH optional
* [#8588](https://github.com/timescale/timescaledb/pull/8588) Create columnstore policy when creating hypertables with columnstore enabled with CREATE TABLE WITH
* [#8606](https://github.com/timescale/timescaledb/pull/8606) Add job history config parameters for maximum successes and failures to keep for each job
* [#8632](https://github.com/timescale/timescaledb/pull/8632) Remove ChunkDispatch custom node
* [#8637](https://github.com/timescale/timescaledb/pull/8637) Add INSERT support for direct compress
* [#8661](https://github.com/timescale/timescaledb/pull/8661) Don't block ALTER TABLE ONLY when settings reloptions
* [#8703](https://github.com/timescale/timescaledb/pull/8703) Allow set-returning functions in continuous aggregates
* [#8734](https://github.com/timescale/timescaledb/pull/8734) Support direct compress when inserting into chunk
* [#8741](https://github.com/timescale/timescaledb/pull/8741) Add support for unlogged hypertables
* [#8769](https://github.com/timescale/timescaledb/pull/8769) Remove continuous aggregate invalidation trigger
* [#8798](https://github.com/timescale/timescaledb/pull/8798) Enable UUID compression by default.
* [#8804](https://github.com/timescale/timescaledb/pull/8804) Remove insert_blocker trigger

**Bugfixes**
* [#8561](https://github.com/timescale/timescaledb/pull/8561) Show warning when direct compress is skipped due to triggers or unique constraints
* [#8567](https://github.com/timescale/timescaledb/pull/8567) Do not require job to have executed to show status
* [#8654](https://github.com/timescale/timescaledb/pull/8654) Fix approximate_row_count for compressed chunks
* [#8704](https://github.com/timescale/timescaledb/pull/8704) Fix direct DELETE on compressed chunk
* [#8728](https://github.com/timescale/timescaledb/pull/8728) Don't block dropping hypertables with other objects
* [#8735](https://github.com/timescale/timescaledb/pull/8735) Fix ColumnarScan for UNION queries
* [#8739](https://github.com/timescale/timescaledb/pull/8739) Fix cached utility statements
* [#8742](https://github.com/timescale/timescaledb/pull/8742) Potential internal program error when grouping by bool columns of a compressed hypertable.
* [#8743](https://github.com/timescale/timescaledb/pull/8743) Modify schedule interval for job history pruning
* [#8746](https://github.com/timescale/timescaledb/pull/8746) Support show/drop chunks with UUIDv7 partitioning
* [#8753](https://github.com/timescale/timescaledb/pull/8753) Allow sorts over decompressed index scans for ChunkAppend
* [#8758](https://github.com/timescale/timescaledb/pull/8758) Improve error message on catalog version mismatch
* [#8774](https://github.com/timescale/timescaledb/pull/8774) Add GUC for WAL based invalidation
* [#8782](https://github.com/timescale/timescaledb/pull/8782) Stops sparse index from allowing multiple options
* [#8799](https://github.com/timescale/timescaledb/pull/8799) Set next_start for WITH clause compression policy
* [#8807](https://github.com/timescale/timescaledb/pull/8807) Only warn but not fail the compression if bloom filter indexes are configured but disabled with a GUC.

**GUCs**

**Thanks**
* @brandonpurcell-dev For highlighting issues with show_chunks() and UUIDv7 partitioning
* @moodgorning for reporting issue with timescaledb_information.job_stats view
* @ruideyllot for reporting set-returning functions not working in continuous aggregates
* @t-aistleitner for reporting an issue with utility statements in plpgsql functions

* [55fd2f2] Fixes command tag return for `INSERT`s on hypertables.
* [c3f930f] Add `time_bucket` functions
* [b128ac2] Fix bug with `INSERT INTO...SELECT`
* [e20edf8] Add better error checking for index creation.
* [72f754a] use PostgreSQL's own `hash_any` function as default partfunc (thanks @robin900)
* [39f4c0f] Remove sample data instructions and point to docs site
* [9015314] Revised the `get_general_index_definition` function to handle cases where indexes have definitions other than just `CREATE INDEX` (thanks @bricklen)