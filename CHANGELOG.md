# TimescaleDB Changelog

**Please note: When updating your database, you should connect using
This page lists all the latest features and updates to TimescaleDB. When you use psql to update your database, use the -X flag and prevent any .psqlrc commands from accidentally triggering the load of a previous DB version.
accidentally triggering the load of a previous DB version.**

## 2.22.1 (2025-09-30)

This release contains performance improvements and bug fixes since the 2.22.0 release. We recommend that you upgrade at the next available opportunity.

**Highlighted features in TimescaleDB v2.22.1**
* 

**Features**

**Bugfixes**
* [#7766](https://github.com/timescale/timescaledb/pull/7766) Load OSM extension in retention background worker to drop tiered chunks
* [#8550](https://github.com/timescale/timescaledb/pull/8550) Error in Gapfill with expressions over aggregates and groupby columns and out-of-order columns
* [#8593](https://github.com/timescale/timescaledb/pull/8593) Error on change of invalidation method for continuous aggregate
* [#8599](https://github.com/timescale/timescaledb/pull/8599) Fix attnum mismatch bug in chunk constraint checks
* [#8607](https://github.com/timescale/timescaledb/pull/8607) Fix interrupted CAgg refresh materialization phase leaving behind pending materialization ranges
* [#8638](https://github.com/timescale/timescaledb/pull/8638) ALTER TABLE RESET for orderby settings
* [#8644](https://github.com/timescale/timescaledb/pull/8644) Fix migration script for sparse index configuration
* [#8657](https://github.com/timescale/timescaledb/pull/8657) Fix CREATE TABLE WITH when using UUID partitioning
* [#8659](https://github.com/timescale/timescaledb/pull/8659) Don't propagate ALTER TABLE commands to FDW chunks
* [#8693](https://github.com/timescale/timescaledb/pull/8693) Compressed index not chosen for varchar segmentby column
* [#8707](https://github.com/timescale/timescaledb/pull/8707) Block concurrent refresh policies for Hierarchical CAggs due to potential deadlocks

**GUCs**

**Thanks**
* @MKrkkl for reporting a bug in Gapfill queries with expressions over aggregates and groupby columns
* @brandonpurcell-dev for creating a test case that showed a bug in CREATE TABLE WITH when using UUID partitioning
* @snyrkill for reporting a bug when interrupting a CAgg refresh

* [c3f930f] Add `time_bucket` functions
* [b128ac2] Fix bug with `INSERT INTO...SELECT`
* [e20edf8] Add better error checking for index creation.
* [72f754a] use PostgreSQL's own `hash_any` function as default partfunc (thanks @robin900)
* [39f4c0f] Remove sample data instructions and point to docs site
* [9015314] Revised the `get_general_index_definition` function to handle cases where indexes have definitions other than just `CREATE INDEX` (thanks @bricklen)
