# TimescaleDB Changelog

**Please note: When updating your database, you should connect using
`psql` with the `-X` flag to prevent any `.psqlrc` commands from
accidentally triggering the load of a previous DB version.**

## 2.21.2 (2025-07-31)

This release contains performance improvements and bug fixes since the 2.21.1 release. We recommend that you upgrade at the next available opportunity.

**Highlighted features in TimescaleDB v2.21.2**
* 

**Features**
* [#8207](https://github.com/timescale/timescaledb/pull/8207) Logical decoding plugin for continuous aggregate invalidations
* [#8306](https://github.com/timescale/timescaledb/pull/8306) Add option for invalidation collection using WAL for continuous aggregates
* [#8340](https://github.com/timescale/timescaledb/pull/8340) Improve selectivity estimates for sparse minmax indexes, so that an index scan on compressed table is chosen more often when it's beneficial.
* [#8364](https://github.com/timescale/timescaledb/pull/8364) Remove hypercore table access method
* [#8371](https://github.com/timescale/timescaledb/pull/8371) Show available timescaledb ALTER options when encountering unsupported options
* [#8376](https://github.com/timescale/timescaledb/pull/8376) Change DecompressChunk custom node name to ColumnarScan
* [#8385](https://github.com/timescale/timescaledb/pull/8385) UUID v7 functions for testing pre PG18
* [#8393](https://github.com/timescale/timescaledb/pull/8393) Add specialized compression for UUIDs. Best suited for UUID v7, but still works with other UUID versions. This is experimental at the moment and backward compatibility is not guaranteed.
* [#8398](https://github.com/timescale/timescaledb/pull/8398) Set default compression settings at compress time
* [#8401](https://github.com/timescale/timescaledb/pull/8401) Support ALTER TABLE RESET for compression settings
* [#8414](https://github.com/timescale/timescaledb/pull/8414) Vectorised filtering of UUID Eq and Ne filters, plus bulk decompression of UUIDs
* [#8424](https://github.com/timescale/timescaledb/pull/8424) Block downgrade when orderby setting is NULL

**Bugfixes**
* [#8336](https://github.com/timescale/timescaledb/pull/8336) Fix generic plans for FK checks and prepared statements
* [#8418](https://github.com/timescale/timescaledb/pull/8418) Fix duplicate constraints in JOIN queries
* [#8426](https://github.com/timescale/timescaledb/pull/8426) Fix chunk skipping min/max calculation

**GUCs**

**Thanks**
* @CodeTherapist for reporting the issue with FK checks not working after several insert statements

* [c3f930f] Add `time_bucket` functions
* [b128ac2] Fix bug with `INSERT INTO...SELECT`
* [e20edf8] Add better error checking for index creation.
* [72f754a] use PostgreSQL's own `hash_any` function as default partfunc (thanks @robin900)
* [39f4c0f] Remove sample data instructions and point to docs site
* [9015314] Revised the `get_general_index_definition` function to handle cases where indexes have definitions other than just `CREATE INDEX` (thanks @bricklen)