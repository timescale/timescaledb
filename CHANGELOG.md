# TimescaleDB Changelog

**Please note: When updating your database, you should connect using
This page lists all the latest features and updates to TimescaleDB. When 
you use psql to update your database, use the -X flag and prevent any .psqlrc 
commands from accidentally triggering the load of a previous DB version.**

## 2.24.0 (2025-11-25)

This release contains performance improvements and bug fixes since the 2.23.1 release. We recommend that you upgrade at the next available opportunity.

**Highlighted features in TimescaleDB v2.24.0**
* 

**Backward-Incompatible Changes**
* [#8761](https://github.com/timescale/timescaledb/pull/8761) Change the version of the bloom filter sparse indexes. The existing indexes will stop working and will require action to re-enable. See the changelog for details.

**Features**
* [#8465](https://github.com/timescale/timescaledb/pull/8465) Speed up the filters like `x = any(array[...])` using bloom filter sparse indexes.
* [#8569](https://github.com/timescale/timescaledb/pull/8569) In-memory recompression
* [#8754](https://github.com/timescale/timescaledb/pull/8754) Add concurrent mode for merging chunks
* [#8786](https://github.com/timescale/timescaledb/pull/8786) Display chunks view range as timestamps for UUIDv7
* [#8819](https://github.com/timescale/timescaledb/pull/8819) Refactor chunk compression logic
* [#8840](https://github.com/timescale/timescaledb/pull/8840) Allow ALTER COLUMN TYPE when compression enabled but no compressed chunks exist
* [#8890](https://github.com/timescale/timescaledb/pull/8890) Declarative Partitioning for Hypertables
* [#8908](https://github.com/timescale/timescaledb/pull/8908) Add time bucketing support for UUIDv7
* [#8909](https://github.com/timescale/timescaledb/pull/8909) Support direct compress on hypertables with continuous aggregates
* [#8939](https://github.com/timescale/timescaledb/pull/8939) Support continuous aggregates on UUIDv7-partitioned hypertables
* [#8959](https://github.com/timescale/timescaledb/pull/8959) Cap continuous aggregate invalidation interval range at chunk boundary
* [#8975](https://github.com/timescale/timescaledb/pull/8975) Exclude date/time columns from default segmentby

**Bugfixes**
* [#8839](https://github.com/timescale/timescaledb/pull/8839) Improve _timescaledb_functions.cagg_watermark error handling
* [#8853](https://github.com/timescale/timescaledb/pull/8853) Change log level of cagg refresh messages to DEBUG1
* [#8933](https://github.com/timescale/timescaledb/pull/8933) Potential crash or seemingly random errors when querying the compressed chunks created on releases before 2.15 and using the minmax sparse indexes.
* [#8942](https://github.com/timescale/timescaledb/pull/8942) Fix lateral join handling for compressed chunks
* [#8958](https://github.com/timescale/timescaledb/pull/8958) Fix if_not_exists behaviour when adding refresh policy
* [#8969](https://github.com/timescale/timescaledb/pull/8969) Gracefully handle missing job stat in background worker

**GUCs**

**Thanks**
* @bezpechno for implementing ALTER COLUMN TYPE for hypertable with columnstore when no compressed chunks exist

* [55fd2f2] Fixes command tag return for `INSERT`s on hypertables.
* [c3f930f] Add `time_bucket` functions
* [b128ac2] Fix bug with `INSERT INTO...SELECT`
* [e20edf8] Add better error checking for index creation.
* [72f754a] use PostgreSQL's own `hash_any` function as default partfunc (thanks @robin900)
* [39f4c0f] Remove sample data instructions and point to docs site
* [9015314] Revised the `get_general_index_definition` function to handle cases where indexes have definitions other than just `CREATE INDEX` (thanks @bricklen)