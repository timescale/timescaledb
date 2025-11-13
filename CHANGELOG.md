# TimescaleDB Changelog

**Please note: When updating your database, you should connect using
This page lists all the latest features and updates to TimescaleDB. When 
you use psql to update your database, use the -X flag and prevent any .psqlrc 
commands from accidentally triggering the load of a previous DB version.**

## 2.23.1 (2025-11-13)

This release contains performance improvements and bug fixes since the 2.23.0 release. We recommend that you upgrade at the next available opportunity.

**Highlighted features in TimescaleDB v2.23.1**
* 

**Features**

**Bugfixes**
* [#8873](https://github.com/timescale/timescaledb/pull/8873) Don't error on failure to update job stats
* [#8875](https://github.com/timescale/timescaledb/pull/8875) Fix decoding of UUID v7 timestamp microseconds
* [#8879](https://github.com/timescale/timescaledb/pull/8879) Fix blocker for multiple hierarchical continuous aggregate policies
* [#8882](https://github.com/timescale/timescaledb/pull/8882) Fix crash in policy creation

**GUCs**

**Thanks**
* @alexanderlaw for reporting a crash when creating a policy
* @leppaott for reporting an issue with hierarchical continuous aggregates

* [55fd2f2] Fixes command tag return for `INSERT`s on hypertables.
* [c3f930f] Add `time_bucket` functions
* [b128ac2] Fix bug with `INSERT INTO...SELECT`
* [e20edf8] Add better error checking for index creation.
* [72f754a] use PostgreSQL's own `hash_any` function as default partfunc (thanks @robin900)
* [39f4c0f] Remove sample data instructions and point to docs site
* [9015314] Revised the `get_general_index_definition` function to handle cases where indexes have definitions other than just `CREATE INDEX` (thanks @bricklen)
