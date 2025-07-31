# TimescaleDB Changelog

**Please note: When updating your database, you should connect using
`psql` with the `-X` flag to prevent any `.psqlrc` commands from
accidentally triggering the load of a previous DB version.**

## 2.21.1 (2025-07-22)

## 2.21.1 (2025-07-31)

This release contains performance improvements and bug fixes since the 2.21.0 release. We recommend that you upgrade at the next available opportunity.

**Highlighted features in TimescaleDB v2.21.1**
* 

**Features**

**Bugfixes**
* [#8418](https://github.com/timescale/timescaledb/pull/8418) Fix duplicate constraints in JOIN queries
* [#8426](https://github.com/timescale/timescaledb/pull/8426) Fix chunk skipping min/max calculation

**GUCs**
* 

**Thanks**

* [1c4868d] Add documentation for chunk_time_interval argument
* [55fd2f2] Fixes command tag return for `INSERT`s on hypertables.
* [c3f930f] Add `time_bucket` functions
* [b128ac2] Fix bug with `INSERT INTO...SELECT`
* [e20edf8] Add better error checking for index creation.
* [72f754a] use PostgreSQL's own `hash_any` function as default partfunc (thanks @robin900)
* [39f4c0f] Remove sample data instructions and point to docs site
* [9015314] Revised the `get_general_index_definition` function to handle cases where indexes have definitions other than just `CREATE INDEX` (thanks @bricklen)