# TimescaleDB Changelog

**Please note: When updating your database, you should connect using
This page lists all the latest features and updates to TimescaleDB. When 
you use psql to update your database, use the -X flag and prevent any .psqlrc 
commands from accidentally triggering the load of a previous DB version.**

## 2.25.0 (2026-01-20)

This release contains performance improvements and bug fixes since the 2.24.0 release. We recommend that you upgrade at the next available opportunity.

**Highlighted features in TimescaleDB v2.25.0**
* 

**Backward-Incompatible Changes**

**Features**
* [#8229](https://github.com/timescale/timescaledb/pull/8229) Removed `time_bucket_ng` function
* [#8777](https://github.com/timescale/timescaledb/pull/8777) Enable direct compress on CAgg refresh using new GUC `timescaledb.enable_direct_compress_on_cagg_refresh`
* [#8859](https://github.com/timescale/timescaledb/pull/8859) Remove Continuous Aggregate old format support
* [#9016](https://github.com/timescale/timescaledb/pull/9016) Remove _timescaledb_debug schema
* [#9017](https://github.com/timescale/timescaledb/pull/9017) move bgw_job table into schema _timescaledb_catalog
* [#9022](https://github.com/timescale/timescaledb/pull/9022) Remove WAL based invalidation
* [#9031](https://github.com/timescale/timescaledb/pull/9031) Change default `buckets_per_batch` on CAgg refresh policy to `10`
* [#9032](https://github.com/timescale/timescaledb/pull/9032) Add in-memory recompression for unordered chunks
* [#9033](https://github.com/timescale/timescaledb/pull/9033) Add rebuild_columnstore procedure
* [#9038](https://github.com/timescale/timescaledb/pull/9038) Change default configuration for compressed continuous aggregates
* [#9042](https://github.com/timescale/timescaledb/pull/9042) Enable batch sorted merge on unordered compressed chunks
* [#9046](https://github.com/timescale/timescaledb/pull/9046) Allow non timescaledb namespace SET option for caggs
* [#9059](https://github.com/timescale/timescaledb/pull/9059) Allow configuring work_mem for bgw jobs
* [#9074](https://github.com/timescale/timescaledb/pull/9074) Add function to estimate uncompressed size of compressed chunk
* [#9085](https://github.com/timescale/timescaledb/pull/9085) Don't register timescaledb-tune specific GUCs
* [#9088](https://github.com/timescale/timescaledb/pull/9088) Add ColumnarIndexScan custom node
* [#9090](https://github.com/timescale/timescaledb/pull/9090) Support direct batch delete on hypertables with continuous aggregates
* [#9094](https://github.com/timescale/timescaledb/pull/9094) Enable the columnar pipeline for grouping without aggregation to speed up the queries of the form `select column from table group by column`.
* [#9103](https://github.com/timescale/timescaledb/pull/9103) Support first/last in ColumnarIndexScan
* [#9108](https://github.com/timescale/timescaledb/pull/9108) Support multiple aggregates in ColumnarIndexScan
* [#9111](https://github.com/timescale/timescaledb/pull/9111) Allow recompression with orderby/index changes
* [#9113](https://github.com/timescale/timescaledb/pull/9113) Use enable_columnarscan to control columnarscan
* [#9127](https://github.com/timescale/timescaledb/pull/9127) Remove primary dimension constraints from fully covered chunks
* Add sql function to fetch cagg grouping columns

**Bugfixes**
* [#8706](https://github.com/timescale/timescaledb/pull/8706) Fix planning performance regression on PG16 and later on some join queries.
* [#8986](https://github.com/timescale/timescaledb/pull/8986) Add pathkey replacement for ColumnarScanPath
* [#8989](https://github.com/timescale/timescaledb/pull/8989) Ensure no XID is assigned during chunk query
* [#8990](https://github.com/timescale/timescaledb/pull/8990) Fix EquivalenceClass index update for RelOptInfo
* [#9007](https://github.com/timescale/timescaledb/pull/9007) Add validation for compression index key limits
* [#9024](https://github.com/timescale/timescaledb/pull/9024) Recompress some chunks on VACUUM FULL
* [#9045](https://github.com/timescale/timescaledb/pull/9045) Fix missing UUID check in compression policy
* [#9056](https://github.com/timescale/timescaledb/pull/9056) Fix split chunk relfrozenxid
* [#9058](https://github.com/timescale/timescaledb/pull/9058) Fix missing chunk column stats bug
* [#9061](https://github.com/timescale/timescaledb/pull/9061) Fix update race with BGW jobs
* [#9069](https://github.com/timescale/timescaledb/pull/9069) Fix applying multikey sort for columnstore when one numeric key is pinned to a Const of different type
* [#9102](https://github.com/timescale/timescaledb/pull/9102) Support retention policies on UUIDv7-partitioned hypertables
* [#9120](https://github.com/timescale/timescaledb/pull/9120) Fix for pre PG17, where a DELETE from a partially compressed chunk may miss records if BitmapHeapScan is being used.
* [#9121](https://github.com/timescale/timescaledb/pull/9121) Allow any immutable constant expressions as default values for compressed columns.
* [#9121](https://github.com/timescale/timescaledb/pull/9121) Fix a potential "unexpected column type 'bool'" error for compressed bool columns with missing value.
* [#9144](https://github.com/timescale/timescaledb/pull/9144) Fix handling implicit constraints in ALTER TABLE

**GUCs**

**Thanks**
* @t-aistleitner for reporting the planning performance regression on PG16 and later on some join queries.
* @vahnrr for reporting a crash when adding columns and constraints to a hypertable at the same time

* [55fd2f2] Fixes command tag return for `INSERT`s on hypertables.
* [c3f930f] Add `time_bucket` functions
* [b128ac2] Fix bug with `INSERT INTO...SELECT`
* [e20edf8] Add better error checking for index creation.
* [72f754a] use PostgreSQL's own `hash_any` function as default partfunc (thanks @robin900)
* [39f4c0f] Remove sample data instructions and point to docs site
* [9015314] Revised the `get_general_index_definition` function to handle cases where indexes have definitions other than just `CREATE INDEX` (thanks @bricklen)