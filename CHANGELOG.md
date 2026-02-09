# TimescaleDB Changelog

**Please note: When updating your database, you should connect using
This page lists all the latest features and updates to TimescaleDB. When 
you use psql to update your database, use the -X flag and prevent any .psqlrc 
commands from accidentally triggering the load of a previous DB version.**

## 2.25.1 (2026-02-09)

This release contains performance improvements and bug fixes since the 2.25.0 release. We recommend that you upgrade at the next available opportunity.

**Highlighted features in TimescaleDB v2.25.1**
* 

**Backward-Incompatible Changes**

**Features**

**Bugfixes**
* [#9215](https://github.com/timescale/timescaledb/pull/9215) Add missing handling for em_parent to sort_transform
* [#9223](https://github.com/timescale/timescaledb/pull/9223) Clean up orphaned entries in continuous aggregate invalidaton logs

**GUCs**

**Thanks**
* @emapple for reporting a crash in a query with nested joins and subqueries

* [55fd2f2] Fixes command tag return for `INSERT`s on hypertables.
* [c3f930f] Add `time_bucket` functions
* [b128ac2] Fix bug with `INSERT INTO...SELECT`
* [e20edf8] Add better error checking for index creation.
* [72f754a] use PostgreSQL's own `hash_any` function as default partfunc (thanks @robin900)
* [39f4c0f] Remove sample data instructions and point to docs site
* [9015314] Revised the `get_general_index_definition` function to handle cases where indexes have definitions other than just `CREATE INDEX` (thanks @bricklen)
