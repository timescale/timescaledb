# TimescaleDB Changelog

**Please note: When updating your database, you should connect using
`psql` with the `-X` flag to prevent any `.psqlrc` commands from
accidentally triggering the load of a previous DB version.**

## 2.21.0 (2025-07-01)

This release contains performance improvements and bug fixes since the 2.20.3 release. We recommend that you upgrade at the next available opportunity.

**Highlighted features in TimescaleDB v2.21.0**
* 

**Features**
* [#8081](https://github.com/timescale/timescaledb/pull/8081) Use JSON error code for job configuration parsing
* [#8100](https://github.com/timescale/timescaledb/pull/8100) Support splitting compressed chunks
* [#8131](https://github.com/timescale/timescaledb/pull/8131) Add policy to process hypertable invalidations
* [#8141](https://github.com/timescale/timescaledb/pull/8141) Add function to process hypertable invalidations
* [#8165](https://github.com/timescale/timescaledb/pull/8165) Reindex recompressed chunks in compression policy
* [#8178](https://github.com/timescale/timescaledb/pull/8178) Add columnstore option to CREATE TABLE WITH
* [#8179](https://github.com/timescale/timescaledb/pull/8179) Implement direct DELETE on non-segmentby columns
* [#8182](https://github.com/timescale/timescaledb/pull/8182) Cache information for repeated upserts into the same compressed chunk.
* [#8187](https://github.com/timescale/timescaledb/pull/8187) Allow concurrent Continuous Aggregate refreshes
* [#8191](https://github.com/timescale/timescaledb/pull/8191) Add option to not process hypertable invalidations
* [#8196](https://github.com/timescale/timescaledb/pull/8196) Show deprecation warning for TAM
* [#8208](https://github.com/timescale/timescaledb/pull/8208) Use NULL compression for BOOL batches with all null values like the other compression algorithms
* [#8223](https://github.com/timescale/timescaledb/pull/8223) Support for Attach/Detach Chunk 
* [#8265](https://github.com/timescale/timescaledb/pull/8265) Set incremental CAgg refresh policy on by default
* [#8274](https://github.com/timescale/timescaledb/pull/8274) Allow creating concurrent continuous aggregate refresh policies
* [#8314](https://github.com/timescale/timescaledb/pull/8314) Add support for timescaledb_lake in loader
* Add experimental support for on-the-fly compression to COPY

**Bugfixes**
* [#8153](https://github.com/timescale/timescaledb/pull/8153) Restoring a database having NULL compressed data
* [#8164](https://github.com/timescale/timescaledb/pull/8164) Check columns when creating new chunk from table
* [#8294](https://github.com/timescale/timescaledb/pull/8294) The "vectorized predicate called for a null value" error for WHERE conditions like `x = any(null::int[])`.
* [#8307](https://github.com/timescale/timescaledb/pull/8307) Fix missing catalog entries for bool and null compression in fresh installations
* [#8323](https://github.com/timescale/timescaledb/pull/8323) Fix DML issue with expression indexes and BHS

**GUCs**
* 

**Thanks**

* [c3f930f] Add `time_bucket` functions
* [b128ac2] Fix bug with `INSERT INTO...SELECT`
* [e20edf8] Add better error checking for index creation.
* [72f754a] use PostgreSQL's own `hash_any` function as default partfunc (thanks @robin900)
* [39f4c0f] Remove sample data instructions and point to docs site
* [9015314] Revised the `get_general_index_definition` function to handle cases where indexes have definitions other than just `CREATE INDEX` (thanks @bricklen)
