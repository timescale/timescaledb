# TimescaleDB Changelog

**Please note: When updating your database, you should connect using
`psql` with the `-X` flag to prevent any `.psqlrc` commands from
accidentally triggering the load of a previous DB version.**

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
