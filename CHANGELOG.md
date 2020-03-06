# TimescaleDB Changelog

**Please note: When updating your database, you should connect using
`psql` with the `-X` flag to prevent any `.psqlrc` commands from
accidentally triggering the load of a previous DB version.**

## latest

**Major Features**

**Minor Features**
* #1666 Support drop_chunks API for continuous aggregates
* #1711 Change log level for continuous aggregate materialization messages

**Bugfixes**
* #1648 Drop chunks from materialized hypertable
* #1665 Add ignore_invalidation_older_than to timescaledb_information.continuous_aggregates view
* #1668 Cannot add dimension if hypertable has empty chunks
* #1674 Fix time_bucket_gapfill's interaction with GROUP BY
* #1686 Fix order by queries on compressed hypertables that have char segment by column
* #1687 Fix issue with disabling compression when foreign keys are present
* #1715 Fix issue with overly aggressive chunk exclusion in outer joins
* #1727 Fix compressing INTERVAL columns
* #1728 Handle Sort nodes in ConstraintAwareAppend
* #1730 Fix partial index handling on chunks

**Licensing changes**
* Reorder and policies around reorder and drop chunks are now
  accessible to community users, not just enterprise
* Gapfill functionality no longer warns about expired license

**Thanks**
* @RJPhillips01 for reporting an issue with drop chunks.
* @b4eEx for reporting an issue with disabling compression.
* @dark048 for reporting an issue with order by on compressed hypertables
* @mrechte for reporting an issue with compressing INTERVAL columns
* @tstaehli for reporting an issue with ConstraintAwareAppend
* @chadshowalter for reporting an issue with partial index on hypertables

## 1.6.0 (2020-01-14)

This release adds major new features and bugfixes since the 1.5.1 release.
We deem it moderate priority for upgrading.

The major new feature in this release allows users to keep the aggregated
data in a continuous aggregate while dropping the raw data with drop_chunks.
This allows users to save storage by keeping only the aggregates.

The semantics of the refresh_lag parameter for continuous aggregates has
been changed to be relative to the current timestamp instead of the maximum
value in the table. This change requires that an integer_now func be set on
hypertables with integer-based time columns to use continuous aggregates on
this table.

We added a timescaledb.ignore_invalidation_older_than parameter for continuous
aggregates. This parameter accept a time-interval (e.g. 1 month). If set,
it limits the amount of time for which to process invalidation. Thus, if
timescaledb.ignore_invalidation_older_than = '1 month', then any modifications
for data older than 1 month from the current timestamp at modification time may
not cause continuous aggregate to be updated. This limits the amount of work
that a backfill can trigger. By default, all invalidations are processed.

**Major Features**
* #1589 Allow drop_chunks while keeping continuous aggregates

**Minor Features**
* #1568 Add ignore_invalidation_older_than option to continuous aggs
* #1575 Reorder group-by clause for continuous aggregates
* #1592 Improve continuous agg user messages

**Bugfixes**
* #1565 Fix partial select query for continuous aggregate
* #1591 Fix locf treat_null_as_missing option
* #1594 Fix error in compression constraint check
* #1603 Add join info to compressed chunk
* #1606 Fix constify params during runtime exclusion
* #1607 Delete compression policy when drop hypertable
* #1608 Add jobs to timescaledb_information.policy_stats
* #1609 Fix bug with parent table in decompression
* #1624 Fix drop_chunks for ApacheOnly
* #1632 Check for NULL before dereferencing variable

**Thanks**
* @optijon for reporting an issue with locf treat_null_as_missing option
* @acarrera42 for reporting an issue with constify params during runtime exclusion
* @ChristopherZellermann for reporting an issue with the compression constraint check
* @SimonDelamare for reporting an issue with joining hypertables with compression

## 1.5.1 (2019-11-12)

This maintenance release contains bugfixes since the 1.5.0 release. We deem it low
priority for upgrading.

In particular the fixes contained in this maintenance release address potential
segfaults and no other security vulnerabilities. The bugfixes are related to bloom
indexes and updates from previous versions.

**Bugfixes**
* #1523 Fix bad SQL updates from previous updates
* #1526 Fix hypertable model
* #1530 Set active snapshots in multi-xact index create

**Thanks**
* @84660320 for reporting an issue with bloom indexes
* @gumshoes @perhamm @jermudgeon @gmisagm for reporting the issue with updates

## 1.5.0 (2019-10-31)

This release adds major new features and bugfixes since the 1.4.2 release.
We deem it moderate priority for upgrading.

This release adds compression as a major new feature.
Multiple type-specific compression options are available in this release
(including DeltaDelta with run-length-encoding for integers and
timestamps; Gorilla compression for floats; dictionary-based compression
for any data type, but specifically for low-cardinality datasets;
and other LZ-based techniques). Individual columns can be compressed with
type-specific compression algorithms as Postgres' native row-based format
are rolled up into columnar-like arrays on a per chunk basis.
The query planner then handles transparent decompression for compressed
chunks at execution time.

This release also adds support for basic data tiering by supporting
the migration of chunks between tablespaces, as well as support for
parallel query coordination to the ChunkAppend node.
Previously ChunkAppend would rely on parallel coordination in the
underlying scans for parallel plans.

More information can be found on [our blog](https://blog.timescale.com/blog/building-columnar-compression-in-a-row-oriented-database)
or in this [tutorial](https://docs.timescale.com/latest/tutorials/compression-tutorial)

**For this release only**, you will need to restart the database before running
`ALTER EXTENSION`

**Major Features**
* #1393 Moving chunks between different tablespaces
* #1433 Make ChunkAppend parallel aware
* #1434 Introducing native compression, multiple compression algorithms, and hybrid row/columnar projections

**Minor Features**
* #1471 Allow setting reloptions on chunks
* #1479 Add next_start option to alter_job_schedule
* #1481 Add last_successful_finish to bgw_job_stats

**Bugfixes**
* #1444 Prevent LIMIT pushdown in JOINs
* #1447 Fix runtime exclusion memory leak
* #1464 Fix ordered append with expressions in ORDER BY clause with space partitioning
* #1476 Fix logic for BGW rescheduling
* #1477 Fix gapfill treat_null_as_missing
* #1493 Prevent recursion in invalidation processing
* #1498 Fix overflow in gapfill's interpolate
* #1499 Fix error for exported_uuid in pg_restore
* #1503 Fix bug with histogram function in parallel

**Thanks**
* @dhyun-obsec for reporting an issue with pg_restore
* @rhaymo for reporting an issue with interpolate
* @optijon for reporting an issue with locf treat_null_as_missing
* @favnee for reporting an issue with runtime exclusion
* @Lectem for reporting an issue with histograms
* @rogerdwan for reporting an issue with BGW rescheduling
* @od0 for reporting an issue with alter_job_schedule

## 1.4.2 (2019-09-11)

This maintenance release contains bugfixes since the 1.4.1 release. We deem it medium
priority for upgrading.

In particular the fixes contained in this maintenance release address 2 potential
segfaults and no other security vulnerabilities. The bugfixes are related to
background workers, OUTER JOINs, ordered append on space partitioned hypertables
and expression indexes.

**Bugfixes**
* #1327 Fix chunk exclusion with ordered append
* #1390 Fix deletes of background workers while a job is running
* #1392 Fix cagg_agg_validate expression handling (segfault)
* #1408 Fix ChunkAppend space partitioning support for ordered append
* #1420 Fix OUTER JOIN qual propagation
* #1422 Fix background worker error handling (segfault)
* #1424 Fix ChunkAppend LIMIT pushdown
* #1429 Fix expression index creation

**Thanks**
* @shahidhk for reporting an issue with OUTER JOINs
* @cossbow and @xxGL1TCHxx for reporting reporting issues with ChunkAppend and space partitioning
* @est for reporting an issue with CASE expressions in continuous aggregates
* @devlucasc for reporting the issue with deleting a background worker while a job is running
* @ryan-shaw for reporting an issue with expression indexes on hypertables with dropped columns

## 1.4.1 (2019-08-01)

This maintenance release contains bugfixes since the 1.4.0 release. We deem it medium
priority for upgrading.

In particular the fixes contained in this maintenance release address 2 potential
segfaults and no other security vulnerabilities. The bugfixes are related to queries
with prepared statements, PL/pgSQL functions and interoperability with other extensions.
More details below.

**Bugfixes**
* #1362 Fix ConstraintAwareAppend subquery exclusion
* #1363 Mark drop_chunks as VOLATILE and not PARALLEL SAFE
* #1369 Fix ChunkAppend with prepared statements
* #1373 Only allow PARAM_EXTERN as time_bucket_gapfill arguments
* #1380 Handle Result nodes gracefully in ChunkAppend

**Thanks**
* @overhacked for reporting an issue with drop_chunks and parallel queries
* @fvannee for reporting an issue with ConstraintAwareAppend and subqueries
* @rrb3942 for reporting a segfault with ChunkAppend and prepared statements
* @mchesser for reporting a segfault with time_bucket_gapfill and subqueries
* @lolizeppelin for reporting and helping debug an issue with ChunkAppend and Result nodes

## 1.4.0 (2019-07-18)

This release contains major new functionality for continuous aggregates
and adds performance improvements for analytical queries.

In version 1.3.0 we added support for continuous aggregates which
was initially limited to one continuous aggregate per hypertable.
With this release, we remove this restriction and allow multiple
continuous aggregates per hypertable.

This release adds a new custom node ChunkAppend that can perform
execution time constraint exclusion and is also used for ordered
append. Ordered append no longer requires a LIMIT clause and now
supports space partitioning and ordering by time_bucket.

**Major features**
* #1270 Use ChunkAppend to replace Append nodes
* #1257 Support for multiple continuous aggregates

**Minor features**
* #1181 Remove LIMIT clause restriction from ordered append
* #1273 Propagate quals to joined hypertables
* #1317 Support time bucket functions in Ordered Append
* #1331 Add warning message for REFRESH MATERIALIZED VIEW
* #1332 Add job statistics columns to timescaledb_information.continuous_aggregate_stats view
* #1326 Add architecture and bit size to telemetry

**Bugfixes**
* #1288 Do not remove Result nodes with one-time filter
* #1300 Fix telemetry report return value
* #1339 Fix continuous agg catalog table insert failure
* #1344 Update continuous agg bgw job start time

**Thanks**
* @ik9999 for reporting a bug with continuous aggregates and negative refresh lag

## 1.3.2 (2019-06-24)

This maintenance release contains bug and security fixes since the 1.3.1 release. We deem it moderate-to-high priority for upgrading.

This release fixes some security vulnerabilities, specifically related to being able to elevate role-based permissions by database users that already have access to the database.  We strongly recommend that users who rely on role-based permissions upgrade to this release as soon as possible.

**Security Fixes**
* #1311 Fix role-based permission checking logic

**Bugfixes**
* #1315 Fix potentially lost invalidations in continuous aggs
* #1303 Fix handling of types with custom time partitioning
* #1299 Arm32: Fix Datum to int cast issue
* #1297 Arm32: Fix crashes due to long handling
* #1019 Add ARM32 tests on travis

**Thanks**
* @hedayat for reporting the error with handling of types with custom time partitioning

## 1.3.1 (2019-06-10)

This maintenance release contains bugfixes since the 1.3.0 release.
We deem it low-to-moderate priority for upgrading.

In particular, the fixes contained in this maintenance release do not address any
security vulnerabilities, while the only one affecting system stability is related
to TimescaleDB running on PostgreSQL 11.  More details below.

**Bugfixes**
* #1220 Fix detecting JOINs for continuous aggs
* #1221 Fix segfault in VACUUM on PG11
* #1228 ARM32 Fix: Pass int64 using Int64GetDatum when a Datum is required
* #1232 Allow Param as time_bucket_gapfill arguments
* #1236 Stop preventing REFRESH in transaction blocks
* #1283 Fix constraint exclusion for OUTER JOIN

**Thanks**
* @od0 for reporting an error with continuous aggs and JOINs
* @rickbatka for reporting an error when using time_bucket_gapfill in functions
* @OneMoreSec for reporting the bug with VACUUM
* @dvdrozdov @od0 @t0k4rt for reporting the issue with REFRESH in transaction blocks
* @mhagander and @devrimgunduz for suggesting adding a CMAKE flag to control the default telemetry level

## 1.3.0 (2019-05-06)

This release contains major new functionality that we call continuous aggregates.

Aggregate queries which touch large swathes of time-series data can take a long
time to compute because the system needs to scan large amounts of data on every
query execution. Our continuous aggregates continuously calculate the
results of a query in the background and materialize the results. Queries to the
continuous aggregate view are then significantly faster as they do not need to
touch the raw data in the hypertable, instead using the pre-computed aggregates
in the view.

Continuous aggregates are somewhat similar to PostgreSQL materialized
views, but unlike a materialized view, continuous
aggregates do not need to be refreshed manually; the view will be refreshed
automatically in the background as new data is added, or old data is
modified. Additionally, it does not need to re-calculate all of the data on
every refresh. Only new and/or invalidated data will be calculated.  Since this
re-aggregation is automatic, it doesn’t add any maintenance burden to your
database.

Our continuous aggregate approach supports high-ingest rates by avoiding the
high-write amplification associated with trigger-based approaches. Instead,
we use invalidation techniques to track what data has changed, and then correct
the materialized aggregate the next time that the automated process executes.

More information can be found on [our docs overview](http://docs.timescale.com/using-timescaledb/continuous-aggregates)
or in this [tutorial](http://docs.timescale.com/tutorials/continuous-aggs-tutorial).

**Major Features**
* #1184 Add continuous aggregate functionality

**Minor Features**
* #1005 Enable creating indexes with one transaction per chunk
* #1007 Remove hypertable parent from query plans
* #1038 Infer time_bucket_gapfill arguments from WHERE clause
* #1062 Make constraint aware append parallel safe
* #1067 Add treat_null_as_missing option to locf
* #1112 Add support for window functions to gapfill
* #1130 Add support for cross datatype chunk exclusion for time types
* #1134 Add support for partitionwise aggregation
* #1153 Add time_bucket support to chunk exclusion
* #1170 Add functions for turning restoring on/off and setting license key
* #1177 Add transformed time_bucket comparison to quals
* #1182 Enable optimizing SELECTs within INSERTs
* #1201 Add telemetry for policies: drop_chunk & reorder

**Bugfixes**
* #1010 Add session locks to CLUSTER
* #1115 Fix ordered append optimization for join queries
* #1123 Fix gapfill with prepared statements
* #1125 Fix column handling for columns derived from GROUP BY columns
* #1132 Adjust ordered append path cost
* #1155 Limit initial max_open_chunks_per_insert to PG_INT16_MAX
* #1167 Fix postgres.conf ApacheOnly license
* #1183 Handle NULL in a check constraint name
* #1195 Fix cascade in scheduled drop chunks
* #1196 Fix UPSERT with prepared statements

**Thanks**
* @spickman for reporting a segfault with ordered append and JOINs
* @comicfans for reporting a performance regression with ordered append
* @Specter-Y for reporting a segfault with UPSERT and prepared statements
* @erthalion submitting a bugfix for a segfault with validating check constraints

## 1.2.2 (2019-03-14)

This release contains bugfixes.

**Bugfixes**
* #1097 Adjust ordered append plan cost
* #1079 Stop background worker on ALTER DATABASE SET TABLESPACE and CREATE DATABASE WITH TEMPLATE
* #1088 Fix ON CONFLICT when using prepared statements and functions
* #1089 Fix compatibility with extensions that define planner_hook
* #1057 Fix chunk exclusion constraint type inference
* #1060 Fix sort_transform optimization

**Thanks**
* @esatterwhite for reporting a bug when using timescaledb with zombodb
* @eeeebbbbrrrr for fixing compatibility with extensions that also define planner_hook
* @naquad for reporting a segfault when using ON conflict in stored procedures
* @aaronkaplan for reporting an issue with ALTER DATABASE SET TABLESPACE
* @quetz for reporting an issue with CREATE DATABASE WITH TEMPLATE
* @nbouscal for reporting an issue with ordered append resulting in bad plans

## 1.2.1 (2019-02-11)

This release contains bugfixes.

**Notable commits**
* [2f6b58a] Fix tlist on hypertable inserts inside CTEs
* [7973b4a] Stop background worker on rename database
* [32cc645] Fix loading the tsl license in parallel workers

**Thanks**

* @jamessewell for reporting and helping debug a segfault in last() [034a0b0]
* @piscopoc for reporting a segfault in time_bucket_gapfill [e6c68f8]

## 1.2.0 (2019-01-29)

**This is our first release to include Timescale-Licensed features, in addition to new Apache-2 capabilities.**

We are excited to be introducing new time-series analytical functions, advanced data lifecycle management capabilities, and improved performance.
- **Time-series analytical functions**: Users can now use our `time_bucket_gapfill` function, to write complex gapfilling, last object carried forward, and interpolation queries.
- **Advanced data lifecycle management**: We are introducing scheduled policies, which use our background worker framework to manage time-series data. In this release, we support scheduled `drop_chunks` and `reorder`.
- **Improved performance**: We added support for ordered appends, which optimize a large range of queries - particularly those that are ordered by time and contain a LIMIT clause. Please note that ordered appends do not support ordering by `time_bucket` at this time.
- **Postgres 11 Support**: We added beta support for PG11 in 1.1.0. We're happy to announce that our PG11 support is now out of beta, and fully supported.

This release adds code under a new license, LICENSE_TIMESCALE. This code can be found in `tsl`.

**For this release only**, you will need to restart the database before running
`ALTER EXTENSION`

**Notable commits**

* [a531733] switch cis state when we switch chunks
* [5c6b619] Make a copy of the ri_onConflict object in PG11
* [61e524e] Make slot for upserts be update for every chunk switch
* [8a7c127] Fix for ExecSlotDescriptor during upserts
* [fa61613] Change time_bucket_gapfill argument names
* [01be394] Fix bgw_launcher restart when failing during launcher setup
* [7b3929e] Add ordered append optimization
* [a69f84c] Fix signal processing in background workers
* [47b5b7d] Log which chunks are dropped by background workers
* [4e1e15f] Add reorder command
* [2e4bb5d] Recluster and drop chunks scheduling code
* [ef43e52] Add alter_policy_schedule API function
* [5ba740e] Add gapfill query support
* [be7c74c] Add logic for automatic DB maintenance functions
* [4ff6ac7] Initial Timescale-Licensed-Module and License-Key Implementation
* [fc42539] Add new top-level licensing information
* [31e9c5b] Fix time column handling in get_create_command
* [1b8ceca] Avoid loading twice in parallel workers and load only from $libdir
* [76d7875] Don't throw errors when extension is loaded but not installed yet
* [eecd845] Add Timescale License (TSL)
* [4b42b30] Free ChunkInsertStates when the es_per_tuple_exprcontext is freed

**Thanks**

* @fordred for reporting our docker-run.sh script was out of date
* @JpWebster for reporting a deadlock between reads an drop_chunks
* @chickenburgers for reporting an issue with our CMake
* Dimtrj and Asbjørn D., on slack, for creating a reproducible testcase for an UPSERT bug
* @skebanga for reporting a loader bug


## 1.1.1 (2018-12-20)

This release contains bugfixes.

**High-level changes**
* Fix issue when upgrading with pg_upgrade
* Fix a segfault that sometimes appeared in long COPYs
* Other bug and stability fixes

**Notable commits**

* [f99b540] Avoid loading twice in parallel workers and load only from $libdir
* [e310f7d] Don't throw errors when extension is loaded but not installed yet
* [8498416] Free ChunkInsertStates when the es_per_tuple_exprcontext is freed
* [937eefe] Set C standard to C11

**Thanks**

* @costigator for reporting the pg_upgrade bug
* @FireAndIce68 for reporting the parallel workers issue
* @damirda for reporting the copy bug

## 1.1.0 (2018-12-13)

Our 1.1 release introduces beta support for PG 11, as well as several performance optimizations aimed at improving chunk exclusion for read queries. We are also packaging our new timescale-tune tool (currently in beta) with our Debian and Linux releases. If you encounter any issues with our beta features, please file a Github issue.

**Potential breaking changes**
- In addition to optimizing first() / last() to utilize indices for non-group-by queries, we adjusted its sorting behavior to match that of PostgreSQL’s max() and min() functions. Previously, if the column being sorted had NULL values, a NULL would be returned. First() and last() now instead ignore NULL values.

**Notable Commits**

* [71f3a0c] Fix Datum conversion issues
* [5aa1eda] Refactor compatibility functions and code to support PG11
* [e4a4f8e] Add support for functions on open (time) dimensions
* [ed5067c] Fix interval_from_now_to_internal timestamptz handling
* [019971c] Optimize FIRST/LAST aggregate functions
* [83014ee] Implement drop_chunks in C
* [9a34028] Implement show_chunks in C and have drop_chunks use it
* [d461959] Add view to show hypertable information
* [35dee48] Remove version-checking from client-side
* [5b6a5f4] Change size utility and job functions to STRICT
* [7e55d91] Add checks for NULL arguments to DDL functions
* [c1db608] Fix upsert TLE translation when mapping variable numbers
* [55a378e] Check extension exists for DROP OWNED and DROP EXTENSION
* [0c8c085] Exclude unneeded chunks for IN/ANY/ALL operators
* [f27c0a3] Move int time_bucket functions with offset to C

**Thanks**
* @did-g for some memory improvements

## 1.0.1 (2018-12-05)

This commit contains bugfixes and optimizations for 1.0.0

**Notable commits**

* [6553aa4] Make a number of size utility functions to `STRICT`
* [bb1d748] Add checks for NULL arguments to `set_adaptive_chunking`, `set_number_partitions`, `set_chunk_time_interval`, `add_dimension`, and `create_hypertable`
* [a534ed4] Fix upsert TLE translation when mapping variable numbers
* [aecd55b] Check extension exists for DROP OWNED and DROP EXTENSION

## 1.0.0 (2018-10-30)

**This is our 1.0 release!**

For notable commits between 0.12.0/0.12.1 and this final 1.0 release, please see previous entries for the release candidates (rc1, rc2, and rc3).

**Thanks**
To all the external contributors who helped us debug the release candidates, as well as anyone who has contributed bug reports, PRs, or feedback on Slack, GitHub, and other channels. All input has been valuable and helped us create the product we have today!

**Potential breaking changes**
* To better align with the ISO standard so that time bucketing starts each week by default on a Monday (rather than Saturday), the `time_bucket` epoch/origin has been changed from January 1, 2000 to January 3, 2000.  The function now includes an `origin` parameter that can be used to adjust this.
* Error codes are now prefixed with `TS` instead of the prior `IO` prefix. If you were checking for these error codes by name, please update your code.


## 1.0.0-rc3 (2018-10-18)

This release is our third 1.0 release candidate. We expect to only merge bug fixes between now and our final 1.0 release. This is a big milestone for us and signifies our maturity and enterprise readiness.

**PLEASE NOTE** that release candidate (rc) builds will only be made available via GitHub and Docker, and _not_ on other release channels. Please help us test these release candidates out if you can!

**Potential breaking change**: Starting with rc2, we updated our error codes to be prefixed with `TS` instead of the old `IO` prefix. If you were checking for these error codes by name, please update your checks.

**Notable commits**
* [f7ba13d] Handle and test tablespace changes to and from the default tablespace
* [9ccda0d] Start stopped workers on restart message
* [3e3bb0c] Add bool created to create_hypertable and add_dimension return value
* [53ff656] Add state machine and polling to launcher
* [d9b2dfe] Change return value of add_dimension to TABLE
* [19299cf] Make all time_bucket function STRICT
* [297d885] Add a version of time_bucket that takes an origin
* [e74be30] Move time_bucket epoch to a Monday
* [46564c1] Handle ALTER SCHEMA RENAME properly
* [a83e283] Change return value of create_hypertable to TABLE
* [aea7c7e] Add GRANTs to update script for pg_dump to work
* [119963a] Replace hardcoded bash path in shell scripts

**Thanks**
* @jesperpedersen for several PRs that help improve documentation and some rough edges
* @did-g for improvements to our build process
* @skebanga for reporting an issue with ALTER SCHEMA RENAME
* @p-alik for suggesting a way to improve our bash scripts' portability
* @mx781 and @HeikoOnnebrink for reporting an issues with permission GRANTs and ownership when using pg_dump


## 1.0.0-rc2 (2018-09-27)

This release is our second 1.0 release candidate. We expect to only merge bug fixes between now and our final 1.0 release. This is a big milestone for us and signifies our maturity and enterprise readiness.

**PLEASE NOTE** that release candidate (rc) builds will only be made available via GitHub and Docker, and _not_ on other release channels. Please help us test these release candidates out if you can!

**Potential breaking change**: We updated our error codes to be prefixed with `TS` instead of the old `IO` prefix. If you were checking for these error codes by name, please update your checks.

**Notable commits**
* [b43574f] Switch 'IO' error prefix to 'TS'
* [9747885] Prefix public C functions with ts_
* [39510c3] Block unknown alter table commands on  hypertables
* [2408a83] Add support for ALTER TABLE SET TABLESPACE on hypertables
* [41d9846] Enclose macro replacement list and arguments in parentheses
* [cc59d51] Replace macro LEAST_TIMESTAMP by a static function
* [281f363] Modify telemetry BGW to run every hour the first 12 hours
* [a09b3ec] Add pg_isolation_regress support to the timescale build system
* [2c267ba] Handle SIGTERM/SIGINT asynchronously
* [5377e2d] Fix use-after-free bug for connections in the telemetry BGW
* [248f662] Fix pg_dump for unprivileged users
* [193fa4a] Stop background workers when extension is DROP OWNED
* [625e3fa] Fix negative value handling in int time_bucket
* [a33224b] Add loader API version function
* [18b8068] Remove unnecessary index on dimension metadata table
* [d09405d] Fix adaptive chunking when hypertables have multiple dimensions
* [a81dc18] Block signals when writing to the log table in tests
* [d5a6392] Fix adaptive chunking so it chooses correct index
* [3489cca] Fix sigterm handling in background jobs
* [2369ae9] Remove !WIN32 for sys/time.h and sys/socket.h, pg provides fills
* [ebbb4ae] Also add sys/time.h for NetBSD. Fixes #700
* [1a9ae17] Fix build on FreeBSD wrt sockets
* [8225cd2] Remove (redefined) macro PG_VERSION and replace with PACKAGE_VERSION
* [2a07cf9] Release SpinLock even when we're about to Error due to over-decrementing
* [b2a15b8] Make sure DB schedulers are not decremented if they were never incremented
* [6731c86] Add support for pre-release version checks

**Thanks**
* @did-g for an improvement to our macros to make compiliers happy
* @mx781 and @HeikoOnnebrink for reporting issues with working with pg_dump fully
* @znbang and @timolson for reporting a bug that was causing telemetry to fail
* @alanhamlett for reporting an issue with spinlocks when handling SIGTERMs
* @oldgreen for reporting an issue with building on NetBSD
* @kev009 for fixing build issues on FreeBSD and NetBSD
* All the others who have helped us test and used these RCs!


## 0.12.1 (2018-09-19)

**High-level changes**

* Fixes for a few issues related to the new scheduler and background worker framework.
* Fixed bug in adaptive chunking where the incorrect index could be used for determining the current interval.
* Improved testing, code cleanup, and other housekeeping.

**Notable commits**
* [0f6f7fc] Fix adaptive chunking so it chooses correct index
* [3ed79ed] Fix sigterm handling in background jobs
* [bea098f] Remove !WIN32 for sys/time.h and sys/socket.h, pg provides fills
* [9f62a1a] Also add sys/time.h for NetBSD. Fixes #700
* [95a982f] Fix build on FreeBSD wrt sockets
* [fcb4a79] Remove (redefined) macro PG_VERSION and replace with PACKAGE_VERSION
* [2634897] Release SpinLock even when we're about to Error due to over-decrementing
* [1f30dbb] Make sure DB schedulers are not decremented if they were never incremented
* [f518cd0] Add support for pre-release version checks
* [acebaea] Don't start schedulers for template databases.
* [f221a12] Fix use-after-free bug in telemetry test
* [0dc5bbb] Use pg_config bindir directory for pg executables

**Thanks**
* @did-g for reporting a use-after-free bug in a test and for improving the robustness of another test
* @kev009 for fixing build issues on FreeBSD and NetBSD


## 1.0.0-rc1 (2018-09-12)

This release is our 1.0 release candidate. We expect to only merge bug fixes between now and our final 1.0 release. This is a big milestone for us and signifies our maturity and enterprise readiness.

**PLEASE NOTE** that release candidate (rc) builds will only be made available via GitHub and Docker, and _not_ on other release channels. Please help us test these release candidates out if you can!


**Notable commits**
* [acebaea] Don't start schedulers for template databases.
* [f221a12] Fix use-after-free bug in telemetry test
* [2092b2a] Fix unused variable warning in Release build
* [0dc5bbb] Use pg_config bindir directory for pg executables

**Thanks**
* @did-g for reporting a use-after-free bug in a test and for improving the robustness of another test


## 0.12.0 (2018-09-10)

**High-level changes**

*Scheduler framework:* This release introduces a background job framework and scheduler. Each database running within a PostgreSQL instance has a scheduler that schedules recurring jobs from a new jobs table while maintaining statistics that inform the scheduler's policy. Future releases will leverage this scheduler framework for more automated management of data retention, archiving, analytics, and the like.

*Telemetry:* Using this new scheduler framework, TimescaleDB databases now send anonymized usage information to a telemetry server via HTTPS, as well as perform version checking to notify users if a newer version is available. For transparency, a new `get_telemetry_report` function details the exact JSON that is sent, and users may also opt out of this telemetry and version check.

*Continued hardening:* This release addresses several issues around more robust backup and recovery, handling large numbers of chunks, and additional test coverage.

**Notable commits**

* [efab2aa] Fix net lib functionality on Windows and improve error handling
* [71589c4] Fix issues when OpenSSL is not available
* [a43cd04] Call the main telemetry function inside BGW executor
* [faf481b] Add telemetry functionality
* [45a2b76] Add new Connection and HTTP libraries
* [b6fe657] Fix max_background_workers guc, errors on EXEC_BACKEND and formatting
* [5d8c7cc] Add a scheduler for background jobs
* [55a7141] Implement a cluster-wide launcher for background workers
* [5bc705f] Update bootstrap to check for cmake and exit if not found
* [98e56dd] Improve show_indexes test func to be more platform agnostic
* [b928caa] Note how to recreate templated files
* [8571e41] Use AttrNumberGetAttrOffset instead of Anum_name - 1 for array indexing
* [d1710ef] Improve regression test script to cleanup more thoroughly
* [fc3677f] Reduce number of open chunks per insert
* [027b7b2] Hide extension symbols by default on Unix platforms
* [6a3abe5] Fix SubspaceStore to ensure max_open_chunks_per_insert is obeyed

**Thanks**

@EvanCarroll for updates to the bootstrap script to check for cmake


## 0.11.0 (2018-08-08)

**High-level changes**

* **Adaptive chunking**: This feature, currently in beta, allows the database to automatically adapt a chunk's time interval, so that users do not need to manually set (and possibly manually change) this interval size. In this release, users can specify either a target chunk data size (in terms of MB or GB), and the chunk's time intervals will be automatically adapted. Alternatively, users can ask the database to just estimate a target size itself based on the platform's available memory and other parameters, and the system will adapt accordingly. This type of automation can simplify initial database test and operations. This feature is default off. Note: The default time interval for non-adaptive chunking has also been changed from 1 month to 1 week.
* **Continued hardening**: This release addresses a number of less frequently used schema modifications, functions, or constraints. Unsupported functions are safely blocked, while we have added support for a number of new types of table alterations. This release also adds additional test coverage.
* Support for additional types of time columns, if they are binary compatible (thanks @fvannee!).

**Notable commits**

* [9ba2e81] Fix segfault with custom partition types
* [7e9bf25] Change default chunk size to one week
* [506fa18] Add tests for custom types
* [1d9ade7] add support for other types as timescale column
* [570f2f8] Validate parameters when creating partition info
* [148f2da] Use shared_buffers as the available cache memory
* [e0a15c1] Add additional comments to explain algorithm
* [d81dccb] Set the default chunk_time_interval to 1 day with adaptive chunking enabled
* [2e7b32c] Add WARNING when doing min-max heap scan for adaptive chunking
* [6b452a8] Update adaptive chunk algorithm to handle very small chunks.
* [9c9cdca] Add support for adaptive chunk sizing
* [7f8d17d] Handle DEFERRED and VALID options for constraints
* [0c5c21b] Block using rules with hypertables
* [37142e9] Block INSERTs on a hypertable's root table
* [4daf087] Fix some ALTER TABLE corner case bugs on hypertables
* [122f5f1] Block replica identity usage with hypertables
* [8bf552e] Block unlogged tables from being used as hypertables
* [a8c637e] Create aggregate functions only once to avoid dependency issues
* [a97f2af] Add support for custom hypertable dimension types
* [dfe026c] Refactor create_hypertable rel access.
* [ed379c3] Validate existing indexes before adding a new dimension
* [1f2d276] Fix and improve show_indexes test support function
* [77b0035] Enforce IMMUTABLE partitioning functions
* [cbc5e60] Block NO INHERIT constraints on hypertables
* [e362e9c] Block mixing hypertables with postgres inheritance
* [011f12b] Add support for CLUSTER ON and SET WITHOUT CLUSTER
* [e947c6b] Improve handling of column settings
* [fc4957b] Update statistics on parent table when doing ANALYZE
* [82942bf] Enable backwards compatibility for loader for 0.9.0 and 0.9.1

**Thanks**

* @Ngalstyan4 and @hjsuh18, our interns, for all of the PRs this summer
* @fvannee for a PR adding support for binary compatible custom types as a time column
* @fmacelw for reporting a bug where first() and last() hold reference across extension update
* @soccerdroid for reporting a corner case bug in ALTER TABLE


## 0.10.1 (2018-07-12)

**High-level changes**
* Improved memory management for long-lived connections.
* Fixed handling of dropping triggers that would lead to orphaned references in pg_depend.
* Fixed pruning in CustomScan when the subplan is not a Scan type that caused a crash with LATERALs.
* Corrected size reporting that was not accurately counting TOAST size
* Updated error messages that more closely conform to PG style.
* Corrected handling of table and schema name changes to chunks; TimescaleDB metadata catalogs are now properly updated

**Notable commits**
* [8b58500] Fix bug where dropping triggers caused dangling references in pg_depend, disallow disabling triggers on hypertables
* [745b8ab] Fixing CustomScan pruning whenever the subplan is NOT of a Scan type.
* [67a8a41] Make chunk identifiers formatting safe using format
* [41af6ff] Fix misreported toast_size in chunk_relation_size funcs
* [4f2f1a6] Update the error messages to conform with the style guide; Fix tests
* [3c28f65] Release cache pin memory
* [abe76fc] Add support for changing chunk schema and name

**Thanks**
* @mfuterko for updating our error messages to conform with PG error message style
* @fvannee for reporting a crash when using certain LATERAL joins with aggregates
* @linba708 for reporting a memory leak with long lived connections
* @phlsmk for reporting an issue where dropping triggers prevented drop_chunks from working due to orphaned dependencies


## 0.10.0 (2018-06-27)

**High-level changes**
* Planning time improvement (**up to 15x**) when a hypertable has many chunks by only expanding (and taking locks on) chunks that will actually be used in a query, rather than on all chunks (as was the default PostgreSQL behavior).
* Smarter use of HashAggregate by teaching the planner to better estimate the number of output rows when using time-based grouping.
* New convenience function for getting the approximate number of rows in a hypertable (`hypertable_approximate_row_count`).
* Fixed support for installing extension into non-`public` schemas
* Other bug fixes and refactorings.

**Notable commits**
* [12bc117] Fix static analyzer warning when checking for index attributes
* [7d9f49b] Fix missing NULL check when creating default indexes
* [2e1f3b9] Improve memory allocation during cache lookups
* [ca6e5ef] Fix upserts on altered tables.
* [2de6b02] Add optimization to use HashAggregate more often
* [4b4211f] Fix some external functions when setting a custom schema
* [b7257fc] Optimize planning times when hypertables have many chunks
* [c660fcd] Add hypertable_approximate_row_count convenience function
* [9ce1576] Fix a compilation issue on pre 9.6.3 versions

**Thanks**
* @viragkothari for suggesting the addition of `hypertable_approximate_row_count` and @fvannee for providing the initial SQL used to build that function
* 'mintekhab' from Slack for reporting a segfault when using upserts on an altered table
* @mmouterde for reporting an issue where the extension implicitly expected to be installed in the `public` schema
* @mfuterko for bringing some potential bugs to our attention via static analysis

## 0.9.2 (2018-05-04)

**High-level changes**
* Fixed handling of `DISCARD ALL` command when parallel workers are involved, which sometimes caused the extension to complain it was not preloaded
* User permission bug fix where users locating TRIGGER permissions in a database could not insert data into a hypertable
* Fixes for some issues with 32-bit architectures

**Notable commits**
* [256b394] Fix parsing of GrantRoleStmt
* [b78953b] Fix datum conversion typo
* [c7283ef] Fix bug with extension loader when DISCARD ALL is executed
* [fe20e48] Fix chunk creation with user that lacks TRIGGER permission

**Thanks**
* @gumshoes, @manigandham, @wallies, & @cjrh for reporting a problem where sometimes the extension would appear to not be preloaded when it actually was
* @thaxy for reporting a permissions issue when user creating a hypertable lacks TRIGGER permission
* @bertmelis for reporting some bugs with 32-bit architectures

## 0.9.1 (2018-03-26)

**High-level changes**
* **For this release only**, you will need to restart the database before
running `ALTER EXTENSION`
* Several edge cases regarding CTEs addressed
* Updated preloader with better error messaging and fixed edge case
* ABI compatibility with latest PostgreSQL to help catch any breaking
changes

**Notable commits**
* [40ce037] Fix crash on explain analyze with insert cte
* [8378beb] Enable hypertable inserts within CTEs
* [bdfda75] Fix double-loading of extension
* [01ea77e] Fix EXPLAIN output for ConstraintAwareAppend inside CTE
* [fc05637] Add no preload error to versioned library.
* [38f8e0c] Add ABI compatibility tests
* [744ca09] Fix Cache Pinning for Subtxns
* [39010db] Move more drops into event trigger system
* [fc36699] Do not fail add_dimension() on non-empty table with 'if_not_exists'

**Thanks**
* @The-Alchemist for pointing out broken links in the README
* @chaintng for pointing out a broken link in the docs
* @jgranstrom for reporting a edge case crash with UPSERTs in CTEs
* @saosebastiao for reporting the lack of an error message when the library is not preloaded and trying to delete/modify a hypertable
* @jbylund for reporting a cache invalidation issue with the preloader

## 0.9.0 (2018-03-05)

**High-level changes**
* Support for multiple extension versions on different databases in the
same PostgreSQL instance. This allows different databases to be updated
independently and provides for smoother updates between versions. No
more spurious errors in the log as the extension is being
updated, and new versions no longer require a restart of the database.
* Streamlined update process for smaller binary/package sizes
* Significant refactoring to simplify and improve codebase, including
improvements to error handling, security/permissions, and more
* Corrections to edge-case scenarios involving dropping schemas,
hypertables, dimensions, and more
* Correctness improvements through propagating reloptions from main
table to chunk tables and blocking `ONLY` commands that try to alter
hypertables (i.e., changes should be applied to chunks as well)
* Addition of a `migrate_data` option to `create_hypertable` to allow
non-empty tables to be turned into hypertables without separate
creation & insertion steps. Note, this option may take a while if the
original table has lots of data
* Support for `ALTER TABLE RENAME CONSTRAINT`
* Support for adjusting the number of partitions for a space dimension
* Improvements to tablespace handling

**Notable commits**
* [4672719] Fix error in handling of RESET ALL
* [9399308] Refactor/simplify update scripts and build process
* [0e79df4] Fix handling of custom SQL-based partitioning functions
* [f13969e] Fix possible memory safety issue and squash valgrind error.
* [ef74491] Migrate table data when creating a hypertable
* [2696582] Move index and constraints drop handling to event trigger
* [d6baccb] Improve tablespace handling, including blocks for DROP and REVOKE
* [b9a6f89] Handle DROP SCHEMA for hypertable and chunk schemas
* [b534a5a] Add test case for adding metadata entries automatically
* [6adce4c] Handle TRUNCATE without upcall and handle ONLY modifier
* [71b1124] Delete orphaned dimension slices
* [fa19a54] Handle deletes on metadata objects via native catalog API
* [6e011d1] Refactor hypertable-related API functions
* [5afd39a] Fix locking for serializing chunk creation
* [6dd2c46] Add check for null in ca_append_rescan to prevent segfault
* [71962b8] Refactor dimension-related API functions
* [cc254a9] Fix CREATE EXTENSION IF NOT EXISTS and error messages
* [d135256] Spread chunk indexes across tablespaces like chunks
* [e85721a] Block ONLY hypertable on all ALTER TABLE commands.
* [78d36b5] Handle subtxn for cache pinning
* [26ef77f] Add subtxn abort logic to process_utility.c
* [25f3284] Handle cache invalidation during subtxn rollback
* [264956f] Block DROP NOT NULL on time-partitioned columns.
* [ad7d361] Better accounting for number of items stored in a subspace
* [12f92ea] Improve speed of out-of-order inserts
* [87f055d] Add support for ALTER TABLE RENAME CONSTRAINT.
* [da8cc79] Add support for multiple extension version in one pg instance
* [68faddc] Make chunks inherit reloptions set on the hypertable
* [4df8f28] Add proper permissions handling for associated (chunk) schemas
* [21efcce] Refactor chunk table creation and unify constraint handling

**Thanks**
* @Anthares for a request to pass reloptions like fill factor to child chunks
* @oldgreen for reporting an issue with subtransaction handling
* @fvannee for a PR that fixed a bug with `ca_append_rescan`
* @maksm90 for reporting an superfluous index being created in an internal catalog table
* @Rashid987 for reporting an issue where deleting a chunk, then changing the time interval would not apply the change when a replacement chunk is created
* RaedA from Slack for reporting compilation issues on Windows between
0.8.0 and this release
* @haohello for a request to adjust the number of partitions for a given dimension
* @LonghronShen and @devereaux for reporting an issue (and submitting a PR) for handling version identification when there is more to the version than just major and minor numbers
* @carlospeon for reporting an issue with dropping hypertables
* @gumshoes, @simpod, @jbylund, and @ryan-shaw for testing a pre-release version to verify our new update path works as expected
* @gumshoes for reporting an issue with `RESET ALL`

## 0.8.0 (2017-12-19)

**High-level changes**
* TimescaleDB now builds and runs on Windows! Now in addition to using
Docker, users can choose to build the extension from source and install
on 64-bit Windows
* Update functions `add_dimension` and `set_chunk_time_interval` to take `INTERVAL` types
* Improved tablespace management including detaching tablespaces from hypertables and looking up tablespaces associated with a hypertable
* Reduced memory usage for `INSERT`s with out-of-order data
* Fixes inserts on 32-bit architectures, in particular ARM
* Other correctness improvements including preventing attachment of
PG10 partitions to hypertables, improved handling of space dimensions
with one partition, and correctly working with `pg_upgrade`
* Test and build improvements making those both more robust and easier
to do

**Notable commits**
* [26971d2] Make `tablespace_show` function return Name instead of CString
* [2fe447b] Make TimescaleDB work with pg_upgrade
* [90c7a6f] Fix logic for one space partition
* [6cfdd79] Prevent native partitioning attachment of hypertables
* [438d79d] Fix trigger relcache handling for COPY
* [cc1ad95] Reduce memory usage for out-of-order inserts
* [a0f62c5] Improve bootstrap script's robustness
* [00a096f] Modify tests to make more platform agnostic
* [0e76b5f] Do not add tablespaces to hypertable objects
* [176b75e] Add command to show tablespaces attached to a hypertable
* [6e92383] Add function to detach tablespaces from hypertables
* [e593876] Refactor tablespace handling
* [c4a46ac] Add hypertable cache lookup on ID/pkey
* [f38a578] Fix handling of long constraint names
* [20c9b28] Unconditionally add pg_config --includedir to src build
* [12dff61] Fixes insert for 32bit architecture
* [e44e47e] Update add_dimension to take INTERVAL times
* [0763e62] Update set_chunk_time_interval to take INTERVAL times
* [87c4b4f] Fix test generator to work for PG 10.1
* [51854ac] Fix error message to reflect that drop_chunks can take a DATE interval
* [66396fb] Add build support for Windows
* [e1a0e81] Refactor and fix cache invalidation

**Thanks**
* @oldgreen for reporting an issue where `COPY` was warning of relcache reference leaks
* @campeterson for pointing out some documentation typos
* @jwdeitch for the PR to prevent attaching PG10 partitions to hypertables
* @vjpr and @sztanpet for reporting bugs and suggesting improvements to the bootstrap script

## 0.7.1 (2017-11-29)

**High-level changes**
* Fix to the migration script for those coming from 0.6.1 (or earlier)
* Fix edge case in `drop_chunks` when hypertable uses `TIMESTAMP` type
* Query planning improvements & fixes
* Permission fixes and support `SET ROLE` functionality

**Notable commits**
* [717299f] Change time handling in drop_chunks for TIMESTAMP times
* [d8ec285] Do not append-optimize plans with result relations (DELETE/UPDATE)
* [30b72ec] Handle empty append plans in ConstraintAwareAppend
* [b35509b] Permission fixes and allow SET ROLE

**Thanks**
* @shaneodonnell for reporting a bug with empty append plans in ConstraintAwareAppend
* @ryan-shaw for reporting a bug with query plans involving result relations and reporting an issue with our 0.6.1 to 0.7.0 migration script


## 0.7.0 (2017-11-21)

**Please note: This update may take a long time (minutes, even hours) to
complete, depending on the size of your database**

**High-level changes**
* **Initial PostgreSQL 10 support**. TimescaleDB now should work on both PostgreSQL 9.6 and 10. As this is our first release supporting PG10, we look forward to community feedback and testing. _Some release channels, like Ubuntu & RPM-based distros will remain on 9.6 for now_
* Support for `CLUSTER` on hypertables to recursively apply to chunks
* Improve constraint handling of edge cases for `DATE` and `TIMESTAMP`
* Fix `range_start` and `range_end` to properly handle the full 32-bit int space
* Allow users to specify their desired partitioning function
* Enforce `NOT NULL` constraint on time columns
* Add testing infrastructure to use Coverity and test PostgreSQL regression tests in TimescaleDB
* Switch to the CMake build system for better cross-platform support
* Several other bug fixes, cleanups, and improvements

**Notable commits**
* [13e1cb5] Add reindex function
* [6594018] Handle when create_hypertable is invoked on partitioned table
* [818bdbc] Add coverity testing
* [5d0cbc1] Recurse CLUSTER command to chunks
* [9c7191e] Change TIMESTAMP partitioning to be completely tz-independent
* [741b256] Mark IMMUTABLE functions as PARALLEL SAFE
* [2ffb30d] Make aggregate serialize and deserialize functions STRICT
* [c552410] Add build target to run the standard PostgreSQL regression tests
* [291050b] Change DATE partitioning to be completely tz-independent
* [ca0968a] Make all partitioning functions take anyelement argument
* [a4e1e32] Change range_start and range_end semantics
* [2dfbc82] Fix off-by-one error on range-end
* [500563f] Add support for PostgreSQL 10
* [201a948] Check that time dimensions are set as NOT NULL.
* [4532650] Allow setting partitioning function
* [4a0a0d8] Fix column type change on plain tables
* [cf009cc] Avoid string conversion in hash partitioning
* [8151098] Improve update testing by adding a rerun test
* [c420c11] Create a catalog entry for constraint-backed indexes
* [ec746d1] Add ability to run regression test locally
* [44f9fec] Add analyze to parallel test for stability
* [9e0422a] Fix bug with pointer assignment after realloc
* [114fa8d] Refactor functions used to recurse DDL commands to chunks
* [b1ec4fa] Refactor build system to use CMake

**Thanks**
* @jgraichen for reporting an issue with `drop_chunks` not accepting `BIGINT`
* @nathansgreen for reporting an edge case with constraints for `TIMESTAMP`
* @jonmd for reporting a similar edge case for `DATE`
* @jwdeitch for a PR to cover an error case in PG10


## 0.6.1 (2017-11-07)

**High-level changes**

* Fix several memory bugs that caused segfaults
* Fix bug when creating expression indexes
* Plug a memory leak with constraint expressions
* Several other bug fixes and stability improvements

**Notable commits**
* [2799075] Fix EXPLAIN for ConstraintAware and MergeAppend
* [8084594] Use per-chunk memory context for cached chunks
* [a13d9de] Do not convert tuples on insert unless needed
* [da09f24] Limit growth of range table during chunk inserts
* [85dee79] Fix issue with creating expression indexes
* [844ff7f] Fix memory leak due to constraint expressions.
* [e90d3ee] Consider precvious CIS state in copy FROM file to rel
* [56d632f] Fix bug with pointer assignment after realloc
* [f97d624] Make event trigger creation idempotent

**Thanks**
* @jwdeitch for submitting a patch to correct behavior in the COPY operation
* @jgraichen for reporting a bug with expression indexes
* @zixet for reporting a memory leak
* @djk447 for reporting a bug in EXPLAIN with ConstraintAware and MergeAppend

## 0.6.0 (2017-10-12)

**High-level changes**

* Fix bugs where hypertable-specific handlers were affecting normal Postgres tables.
* Make it so that all TimescaleDB commands can run as a normal user rather than a superuser.
* Updates to the code to make the extension compileable on Windows; future changes will add steps to properly build.
* Move `time_bucket` functions out of `public` schema (put in schema where extension is).
* Several other bugs fixes.

**Notable commits**
* [1d73fb8] Fix bug with extension starting too early.
* [fd390ec] Fix chunk index attribute mismatch and locking issue
* [430ed8a] Fix bug with collected commands in index statement.
* [614c2b7] Fix permissions bugs and run tests as normal user
* [ce12104] Fix "ON CONFLICT ON CONSTRAINT" on plain PostgreSQL tables
* [4c451e0] Fix rename and reindex bugs when objects are not relations
* [c3ebc67] Fix permission problems with dropping hypertables and chunks
* [040e815] Remove truncate and hypertable metadata triggers
* [5c26328] Fix INSERT on hypertables using sub-selects with aggregates
* [b57e2bf] Prepare C code for compiling on Windows
* [a2bad2b] Fix constraint validation on regular tables
* [fb5717f] Remove explicit schema for time_bucket
* [04d01ce] Split DDL processing into start and end hooks

**Thanks**
* @oldgreen for reporting `time_bucket` being incorrectly put in the `public` schema and pointing out permission problems
* @qlandman for reporting a bug with INSERT using sub-selects with aggregates
* @min-mwei for reporting a deadlock issue during INSERTs
* @ryan-shaw for reporting a bug where the extension sometimes used `pg_cache` too soon

## 0.5.0 (2017-09-20)

**High-level changes**
* Improved support for primary-key, foreign-key, unique, and exclusion constraints.
* New histogram function added for getting the frequency of a column's values.
* Add support for using `DATE` as partition column.
* `chunk_time_interval` now supports `INTERVAL` data types
* Block several unsupported and/or dangerous operations on hypertables and chunks, including dropping or otherwise altering a chunk directly.
* Several bug fixes throughout the code.

**Notable commits**
* [afcb0b1] Fix NULL handling in first/last functions.
* [d53c705] Add script to dump meta data that can be useful for debugging.
* [aa904fa] Block adding constraints without a constraint name
* [a13039f] Fix dump and restore for tables with triggers and constraints
* [8cf8d3c] Improve the size utils functions.
* [2767548] Block adding constraints using an existing index
* [5cee104] Allow chunk_time_interval to be specified as an INTERVAL type
* [6232f98] Add histogram function.
* [2380033] Block ALTER TABLE and handle DROP TABLE on chunks
* [72d6681] Move security checks for ALTER TABLE ALTER COLUMN to C
* [19d3d89] Handle changing the type of dimension columns correctly.
* [17c4ba9] Handle ALTER TABLE rename column
* [66932cf] Forbid relocating extension after install.
* [d2561cc] Add ability to partition by a date type
* [48e0a61] Remove triggers from chunk and chunk_constraint
* [4dcbe61] Add support for hypertable constraints

**Thanks**
* @raycheung for reporting a segfault in `first`/`last`
* @meotimdihia, @noyez, and @andrew-blake for reporting issues with `UNQIUE` and other types of constraints


## 0.4.2 (2017-09-06)

**High-level changes**
* Provide scripts for backing up and restoring single hypertables

**Notable commits**
* [683c078] Add backup/restore scripts for single hypertables

## 0.4.1 (2017-09-04)

**High-level changes**
* Bug fix for a segmentation fault in the planner
* Shortcut when constraint-aware append excludes all chunks
* Fix edge case with negative timestamps when points fell right on the boundary
* Fix behavior of `time_bucket` for `DATE` types by not converting to `TIMESTAMPTZ`
* Make the output of `chunk_relation_size` consistent

**Notable commits**
* [50c8c4c] Fix possible segfault in planner
* [e49e45c] Fix failure when constraint-aware append excludes all chunks
* [c3b6fb9] Fix bug with negative dimension values
* [3c69e4f] Fix semantics of time_bucket on DATE input
* [0137c92] Fix output order of chunk dimensions and ranges in chunk_relation_size.
* [645b530] Convert inserted tuples to the chunk's rowtype

**Thanks**
* @yadid for reporting a segfault (fixed in 50c8c4c)
* @ryan-shaw for reporting tuples not being correctly converted to a chunk's rowtype (fixed in 645b530)

## 0.4.0 (2017-08-21)

**High-level changes**
* Exclude chunks when constraints can be constifyed even if they are
considered mutable like `NOW()`.
* Support for negative values in the dimension range which allows for pre-1970 dates.
* Improve handling of default chunk times for integral date times by forcing it to be explicit rather than guessing the units of the time.
* Improve memory usage for long-running `COPY` operations (previously it would grow unbounded).
* `VACUUM` and `REINDEX` on hypertables now recurse down to chunks.

**Notable commits**
* [139fe34] Implement constraint-aware appends to exclude chunks at execution time
* [2a51cf0] Add support for negative values in dimension range
* [f2d5c3f] Error if add_dimension given both partition number and interval length
* [f3df02d] Improve handling of non-TIMESTAMP/TZ timestamps
* [6a5a7eb] Reduce memory usage on long-running COPY operations
* [953346c] Make VACUUM and REINDEX recurse to chunks
* [55bfdf7] Release all cache pins when a transaction ends

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
