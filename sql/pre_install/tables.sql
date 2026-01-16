-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--NOTICE: UPGRADE-SCRIPT-NEEDED contents in this file are not auto-upgraded.
-- This file contains table definitions for various abstractions and data
-- structures for representing hypertables and lower-level concepts.
-- Hypertable
-- ==========
--
-- The hypertable is an abstraction that represents a table that is
-- partitioned in N dimensions, where each dimension maps to a column
-- in the table. A dimension can either be 'open' or 'closed', which
-- reflects the scheme that divides the dimension's keyspace into
-- "slices".
--
-- Conceptually, a partition -- called a "chunk", is a hypercube in
-- the N-dimensional space. A chunk stores a subset of the
-- hypertable's tuples on disk in its own distinct table. The slices
-- that span the chunk's hypercube each correspond to a constraint on
-- the chunk's table, enabling constraint exclusion during queries on
-- the hypertable's data.
--
--
-- Open dimensions
------------------
-- An open dimension does on-demand slicing, creating a new slice
-- based on a configurable interval whenever a tuple falls outside the
-- existing slices. Open dimensions fit well with columns that are
-- incrementally increasing, such as time-based ones.
--
-- Closed dimensions
--------------------
-- A closed dimension completely divides its keyspace into a
-- configurable number of slices. The number of slices can be
-- reconfigured, but the new partitioning only affects newly created
-- chunks.
-- The unique constraint is table_name +schema_name. The ordering is
-- important as we want index access when we filter by table_name
CREATE SEQUENCE _timescaledb_catalog.hypertable_id_seq MINVALUE 1;

CREATE TABLE _timescaledb_catalog.hypertable (
  id INTEGER NOT NULL DEFAULT nextval('_timescaledb_catalog.hypertable_id_seq'),
  schema_name name NOT NULL,
  table_name name NOT NULL,
  associated_schema_name name NOT NULL,
  associated_table_prefix name NOT NULL,
  num_dimensions smallint NOT NULL,
  chunk_sizing_func_schema name NOT NULL,
  chunk_sizing_func_name name NOT NULL,
  chunk_target_size bigint NOT NULL, -- size in bytes
  compression_state smallint NOT NULL DEFAULT 0,
  compressed_hypertable_id integer,
  status int NOT NULL DEFAULT 0,
  -- table constraints
  CONSTRAINT hypertable_pkey PRIMARY KEY (id),
  CONSTRAINT hypertable_associated_schema_name_associated_table_prefix_key UNIQUE (associated_schema_name, associated_table_prefix),
  CONSTRAINT hypertable_table_name_schema_name_key UNIQUE (table_name, schema_name),
  CONSTRAINT hypertable_schema_name_check CHECK (schema_name != '_timescaledb_catalog'),
  -- internal compressed hypertables have compression state = 2
  CONSTRAINT hypertable_dim_compress_check CHECK (num_dimensions > 0 OR compression_state = 2),
  CONSTRAINT hypertable_chunk_target_size_check CHECK (chunk_target_size >= 0),
  CONSTRAINT hypertable_compress_check CHECK ( (compression_state = 0 OR compression_state = 1 )  OR (compression_state = 2 AND compressed_hypertable_id IS NULL)),
  CONSTRAINT hypertable_compressed_hypertable_id_fkey FOREIGN KEY (compressed_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id)
);
ALTER SEQUENCE _timescaledb_catalog.hypertable_id_seq OWNED BY _timescaledb_catalog.hypertable.id;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable_id_seq', '');

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable', 'WHERE id >= 1');

-- The tablespace table maps tablespaces to hypertables.
-- This allows spreading a hypertable's chunks across multiple disks.
CREATE TABLE _timescaledb_catalog.tablespace (
  id serial NOT NULL,
  hypertable_id int NOT NULL,
  tablespace_name name NOT NULL,
  -- table constraints
  CONSTRAINT tablespace_pkey PRIMARY KEY (id),
  CONSTRAINT tablespace_hypertable_id_tablespace_name_key UNIQUE (hypertable_id, tablespace_name),
  CONSTRAINT tablespace_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.tablespace', '');

-- A dimension represents an axis along which data is partitioned.
CREATE TABLE _timescaledb_catalog.dimension (
  id serial NOT NULL ,
  hypertable_id integer NOT NULL,
  column_name name NOT NULL,
  column_type REGTYPE NOT NULL,
  aligned boolean NOT NULL,
  -- closed dimensions
  num_slices smallint NULL,
  partitioning_func_schema name NULL,
  partitioning_func name NULL,
  -- open dimensions (e.g., time)
  -- Origin for chunk alignment, stored as Unix epoch microseconds (not PostgreSQL epoch).
  -- For timestamp types: microseconds since 1970-01-01 00:00:00 UTC.
  -- For integer types: the raw integer value (currently not supported).
  -- NULL means use the default origin (0 for legacy chunking, 2001-01-01 for calendar chunking).
  interval_origin bigint NULL,
  interval interval NULL, -- calendar-based interval (variable-length, e.g., '1 month').
  interval_length bigint NULL, -- fixed-size interval in microseconds for timestamp types, or raw value for integer types.
  -- compress interval is used by rollup procedure during compression
  -- in order to merge multiple chunks into a single one
  compress_interval interval NULL,
  compress_interval_length bigint NULL,
  integer_now_func_schema name NULL,
  integer_now_func name NULL,
  -- table constraints
  CONSTRAINT dimension_pkey PRIMARY KEY (id),
  CONSTRAINT dimension_hypertable_id_column_name_key UNIQUE (hypertable_id, column_name),
  CONSTRAINT dimension_check CHECK ((partitioning_func_schema IS NULL AND partitioning_func IS NULL) OR (partitioning_func_schema IS NOT NULL AND partitioning_func IS NOT NULL)),
  CONSTRAINT dimension_check1 CHECK ((num_slices IS NULL AND (interval_length IS NOT NULL OR interval IS NOT NULL)) OR (num_slices IS NOT NULL AND interval_length IS NULL AND interval IS NULL)),
  CONSTRAINT dimension_check2 CHECK ((integer_now_func_schema IS NULL AND integer_now_func IS NULL) OR (integer_now_func_schema IS NOT NULL AND integer_now_func IS NOT NULL)),
  CONSTRAINT dimension_interval_length_check CHECK (interval_length IS NULL OR interval_length > 0),
  CONSTRAINT dimension_compress_interval_length_check CHECK (compress_interval_length IS NULL OR compress_interval_length > 0),
  CONSTRAINT dimension_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension', '');

SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.dimension', 'id'), '');

-- A dimension slice defines a keyspace range along a dimension
-- axis. A chunk references a slice in each of its dimensions, forming
-- a hypercube.
CREATE TABLE _timescaledb_catalog.dimension_slice (
  id serial NOT NULL,
  dimension_id integer NOT NULL,
  range_start bigint NOT NULL,
  range_end bigint NOT NULL,
  -- table constraints
  CONSTRAINT dimension_slice_pkey PRIMARY KEY (id),
  CONSTRAINT dimension_slice_dimension_id_range_start_range_end_key UNIQUE (dimension_id, range_start, range_end),
  CONSTRAINT dimension_slice_check CHECK (range_start <= range_end),
  CONSTRAINT dimension_slice_dimension_id_fkey FOREIGN KEY (dimension_id) REFERENCES _timescaledb_catalog.dimension (id) ON DELETE CASCADE
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension_slice', '');

SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.dimension_slice', 'id'), '');

-- A chunk is a partition (hypercube) in an N-dimensional
-- hyperspace. Each chunk is associated with N constraints that define
-- the chunk's hypercube. Tuples that fall within the chunk's
-- hypercube are stored in the chunk's data table, as given by
-- 'schema_name' and 'table_name'.
CREATE SEQUENCE _timescaledb_catalog.chunk_id_seq MINVALUE 1;

CREATE TABLE _timescaledb_catalog.chunk (
  id integer NOT NULL DEFAULT nextval('_timescaledb_catalog.chunk_id_seq'),
  hypertable_id int NOT NULL,
  schema_name name NOT NULL,
  table_name name NOT NULL,
  compressed_chunk_id integer ,
  dropped boolean NOT NULL DEFAULT FALSE,
  status integer NOT NULL DEFAULT 0,
  osm_chunk boolean NOT NULL DEFAULT FALSE,
  creation_time timestamptz NOT NULL,
  -- table constraints
  CONSTRAINT chunk_pkey PRIMARY KEY (id),
  CONSTRAINT chunk_schema_name_table_name_key UNIQUE (schema_name, table_name),
  CONSTRAINT chunk_compressed_chunk_id_fkey FOREIGN KEY (compressed_chunk_id) REFERENCES _timescaledb_catalog.chunk (id),
  CONSTRAINT chunk_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id)
);
ALTER SEQUENCE _timescaledb_catalog.chunk_id_seq OWNED BY _timescaledb_catalog.chunk.id;

CREATE INDEX chunk_hypertable_id_idx ON _timescaledb_catalog.chunk (hypertable_id);
CREATE INDEX chunk_compressed_chunk_id_idx ON _timescaledb_catalog.chunk (compressed_chunk_id);
--we could use a partial index (where osm_chunk is true). However, the catalog code
--does not work with partial/functional indexes. So we instead have a full index here.
--Another option would be to use the status field to identify a OSM chunk. However bit
--operations only work on varbit datatype and not integer datatype.
CREATE INDEX chunk_osm_chunk_idx ON _timescaledb_catalog.chunk (osm_chunk, hypertable_id);
CREATE INDEX chunk_hypertable_id_creation_time_idx ON _timescaledb_catalog.chunk(hypertable_id, creation_time);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk', '');
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_id_seq', '');

-- A chunk constraint maps a dimension slice to a chunk. Each
-- constraint associated with a chunk will also be a table constraint
-- on the chunk's data table.
CREATE TABLE _timescaledb_catalog.chunk_constraint (
  chunk_id integer NOT NULL,
  dimension_slice_id integer NULL,
  constraint_name name NOT NULL,
  hypertable_constraint_name name NULL,
  -- table constraints
  CONSTRAINT chunk_constraint_chunk_id_constraint_name_key UNIQUE (chunk_id, constraint_name),
  CONSTRAINT chunk_constraint_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id),
  CONSTRAINT chunk_constraint_dimension_slice_id_fkey FOREIGN KEY (dimension_slice_id) REFERENCES _timescaledb_catalog.dimension_slice (id)
);

CREATE INDEX chunk_constraint_dimension_slice_id_idx ON _timescaledb_catalog.chunk_constraint (dimension_slice_id);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_constraint', '');

CREATE SEQUENCE _timescaledb_catalog.chunk_constraint_name;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_constraint_name', '');

-- Track statistics for columns of chunks from a hypertable.
-- Currently, we track the min/max range for a given column across chunks.
-- More statistics (like bloom filters) can be added in the future.
--
-- A "special" entry for a column with invalid chunk_id, PG_INT64_MAX,
-- PG_INT64_MIN indicates that min/max ranges could be computed for this column
-- for chunks.
--
-- The ranges can overlap across chunks. The values could be out-of-date if
-- modifications/changes occur in the corresponding chunk and such entries
-- should be marked as "invalid" to ensure that the chunk is in
-- appropriate state to be able to use these values. Thus these entries
-- are different from dimension_slice which is used for tracking partitioning
-- column ranges which have different characteristics.
--
-- Currently this catalog supports datatypes like INT, SERIAL, BIGSERIAL,
-- DATE, TIMESTAMP etc. by storing the ranges in bigint columns. In the
-- future, we could support additional datatypes (which support btree style
-- >, <, = comparators) by storing their textual representation.
--
CREATE TABLE _timescaledb_catalog.chunk_column_stats (
  id serial NOT NULL,
  hypertable_id integer NOT NULL,
  chunk_id integer NULL,
  column_name name NOT NULL,
  range_start bigint NOT NULL,
  range_end bigint NOT NULL,
  valid boolean NOT NULL,
  -- table constraints
  CONSTRAINT chunk_column_stats_pkey PRIMARY KEY (id),
  CONSTRAINT chunk_column_stats_ht_id_chunk_id_colname_key UNIQUE (hypertable_id, chunk_id, column_name),
  CONSTRAINT chunk_column_stats_range_check CHECK (range_start <= range_end),
  CONSTRAINT chunk_column_stats_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id),
  CONSTRAINT chunk_column_stats_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id)
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_column_stats', '');

SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.chunk_column_stats', 'id'), '');

-- Default jobs are given the id space [1,1000). User-installed jobs and any jobs created inside tests
-- are given the id space [1000, INT_MAX). That way, we do not pg_dump jobs that are always default-installed
-- inside other .sql scripts. This avoids insertion conflicts during pg_restore.
CREATE SEQUENCE _timescaledb_catalog.bgw_job_id_seq
MINVALUE 1000;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.bgw_job_id_seq', '');

  -- We put columns that can be null or have variable length
  -- last. This allow us to read the important fields above in the
  -- scheduler without materializing these fields below, which the
  -- scheduler does not neeed.
CREATE TABLE _timescaledb_catalog.bgw_job (
  id integer NOT NULL DEFAULT nextval('_timescaledb_catalog.bgw_job_id_seq'),
  application_name name NOT NULL,
  schedule_interval interval NOT NULL,
  max_runtime interval NOT NULL,
  max_retries integer NOT NULL,
  retry_period interval NOT NULL,
  proc_schema name NOT NULL,
  proc_name name NOT NULL,
  owner regrole NOT NULL DEFAULT pg_catalog.quote_ident(current_role)::regrole,
  scheduled bool NOT NULL DEFAULT TRUE,
  fixed_schedule bool not null default true,
  initial_start timestamptz,
  hypertable_id integer,
  config jsonb,
  check_schema name,
  check_name name,
  timezone text,
  -- table constraints
  CONSTRAINT bgw_job_pkey PRIMARY KEY (id),
  CONSTRAINT bgw_job_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE
);

ALTER SEQUENCE _timescaledb_catalog.bgw_job_id_seq OWNED BY _timescaledb_catalog.bgw_job.id;

CREATE INDEX bgw_job_proc_hypertable_id_idx ON _timescaledb_catalog.bgw_job (proc_schema, proc_name, hypertable_id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.bgw_job', 'WHERE id >= 1000');

CREATE TABLE _timescaledb_internal.bgw_job_stat (
  job_id integer NOT NULL,
  last_start timestamptz NOT NULL DEFAULT NOW(),
  last_finish timestamptz NOT NULL,
  next_start timestamptz NOT NULL,
  last_successful_finish timestamptz NOT NULL,
  last_run_success bool NOT NULL,
  total_runs bigint NOT NULL,
  total_duration interval NOT NULL,
  total_duration_failures interval NOT NULL,
  total_successes bigint NOT NULL,
  total_failures bigint NOT NULL,
  total_crashes bigint NOT NULL,
  consecutive_failures int NOT NULL,
  consecutive_crashes int NOT NULL,
  flags int NOT NULL DEFAULT 0,
  -- table constraints
  CONSTRAINT bgw_job_stat_pkey PRIMARY KEY (job_id),
  CONSTRAINT bgw_job_stat_job_id_fkey FOREIGN KEY (job_id) REFERENCES _timescaledb_catalog.bgw_job (id) ON DELETE CASCADE
);

CREATE SEQUENCE _timescaledb_internal.bgw_job_stat_history_id_seq MINVALUE 1;

CREATE TABLE _timescaledb_internal.bgw_job_stat_history (
  id BIGINT NOT NULL DEFAULT nextval('_timescaledb_internal.bgw_job_stat_history_id_seq'),
  job_id INTEGER NOT NULL,
  pid INTEGER,
  execution_start TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  execution_finish TIMESTAMPTZ,
  succeeded boolean,
  data jsonb,
  -- table constraints
  CONSTRAINT bgw_job_stat_history_pkey PRIMARY KEY (id)
);

ALTER SEQUENCE _timescaledb_internal.bgw_job_stat_history_id_seq OWNED BY _timescaledb_internal.bgw_job_stat_history.id;

CREATE INDEX bgw_job_stat_history_job_id_idx ON _timescaledb_internal.bgw_job_stat_history (job_id);

--The job_stat table is not dumped by pg_dump on purpose because
--the statistics probably aren't very meaningful across instances.
-- Now we define a special stats table for each job/chunk pair. This will be used by the scheduler
-- to determine whether to run a specific job on a specific chunk.
CREATE TABLE _timescaledb_internal.bgw_policy_chunk_stats (
  job_id integer NOT NULL,
  chunk_id integer NOT NULL,
  num_times_job_run integer,
  last_time_job_run timestamptz,
  -- table constraints
  CONSTRAINT bgw_policy_chunk_stats_job_id_chunk_id_key UNIQUE (job_id, chunk_id),
  CONSTRAINT bgw_policy_chunk_stats_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE,
  CONSTRAINT bgw_policy_chunk_stats_job_id_fkey FOREIGN KEY (job_id) REFERENCES _timescaledb_catalog.bgw_job (id) ON DELETE CASCADE
);

CREATE TABLE _timescaledb_catalog.metadata (
  key NAME NOT NULL,
  value text NOT NULL,
  include_in_telemetry boolean NOT NULL,
  -- table constraints
  CONSTRAINT metadata_pkey PRIMARY KEY (key)
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.metadata', $$ WHERE key <> 'uuid' $$);

-- Log with events that will be sent out with the telemetry. The log
-- will be flushed after it has been sent out. We do not save it to
-- backups since it should not contain important data.
CREATE TABLE _timescaledb_catalog.telemetry_event (
       created timestamptz NOT NULL DEFAULT current_timestamp,
       tag name NOT NULL,
       body jsonb NOT NULL
);

CREATE TABLE _timescaledb_catalog.continuous_agg (
  mat_hypertable_id integer NOT NULL,
  raw_hypertable_id integer NOT NULL,
  parent_mat_hypertable_id integer,
  user_view_schema name NOT NULL,
  user_view_name name NOT NULL,
  partial_view_schema name NOT NULL,
  partial_view_name name NOT NULL,
  direct_view_schema name NOT NULL,
  direct_view_name name NOT NULL,
  materialized_only bool NOT NULL DEFAULT FALSE,
  -- table constraints
  CONSTRAINT continuous_agg_pkey PRIMARY KEY (mat_hypertable_id),
  CONSTRAINT continuous_agg_partial_view_schema_partial_view_name_key UNIQUE (partial_view_schema, partial_view_name),
  CONSTRAINT continuous_agg_user_view_schema_user_view_name_key UNIQUE (user_view_schema, user_view_name),
  CONSTRAINT continuous_agg_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  CONSTRAINT continuous_agg_raw_hypertable_id_fkey FOREIGN KEY (raw_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  CONSTRAINT continuous_agg_parent_mat_hypertable_id_fkey FOREIGN KEY (parent_mat_hypertable_id)
    REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE
);

CREATE INDEX continuous_agg_raw_hypertable_id_idx ON _timescaledb_catalog.continuous_agg (raw_hypertable_id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_agg', '');

-- See the comments for ContinuousAggsBucketFunction structure.
CREATE TABLE _timescaledb_catalog.continuous_aggs_bucket_function (
  mat_hypertable_id integer NOT NULL,
  -- The bucket function
  bucket_func text NOT NULL,
  -- `bucket_width` argument of the function, e.g. "1 month"
  bucket_width text NOT NULL,
  -- optional `origin` argument of the function provided by the user
  bucket_origin text,
  -- optional `offset` argument of the function provided by the user
  bucket_offset text,
  -- optional `timezone` argument of the function provided by the user
  bucket_timezone text,
  -- fixed or variable sized bucket
  bucket_fixed_width bool NOT NULL,
  -- table constraints
  CONSTRAINT continuous_aggs_bucket_function_pkey PRIMARY KEY (mat_hypertable_id),
  CONSTRAINT continuous_aggs_bucket_function_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  CONSTRAINT continuous_aggs_bucket_function_func_check CHECK (pg_catalog.to_regprocedure(bucket_func) IS DISTINCT FROM 0)
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_bucket_function', '');

CREATE TABLE _timescaledb_catalog.continuous_aggs_invalidation_threshold (
  hypertable_id integer NOT NULL,
  watermark bigint NOT NULL,
  -- table constraints
  CONSTRAINT continuous_aggs_invalidation_threshold_pkey PRIMARY KEY (hypertable_id),
  CONSTRAINT continuous_aggs_invalidation_threshold_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_invalidation_threshold', '');

CREATE TABLE _timescaledb_catalog.continuous_aggs_watermark (
  mat_hypertable_id integer NOT NULL,
  watermark bigint NOT NULL,
  -- table constraints
  CONSTRAINT continuous_aggs_watermark_pkey PRIMARY KEY (mat_hypertable_id),
  CONSTRAINT continuous_aggs_watermark_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_watermark', '');



-- this does not have an FK on the materialization table since INSERTs to this
-- table are performance critical
CREATE TABLE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log (
  hypertable_id integer NOT NULL,
  lowest_modified_value bigint NOT NULL,
  greatest_modified_value bigint NOT NULL
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_hypertable_invalidation_log', '');

CREATE INDEX continuous_aggs_hypertable_invalidation_log_idx ON _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log (hypertable_id, lowest_modified_value ASC);

-- per cagg copy of invalidation log
CREATE TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log (
  materialization_id integer,
  lowest_modified_value bigint NOT NULL,
  greatest_modified_value bigint NOT NULL,
  -- table constraints
  CONSTRAINT continuous_aggs_materialization_invalid_materialization_id_fkey FOREIGN KEY (materialization_id) REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_materialization_invalidation_log', '');

CREATE INDEX continuous_aggs_materialization_invalidation_log_idx ON _timescaledb_catalog.continuous_aggs_materialization_invalidation_log (materialization_id, lowest_modified_value ASC);

-- cagg materialization ranges
CREATE TABLE _timescaledb_catalog.continuous_aggs_materialization_ranges (
  materialization_id integer,
  lowest_modified_value bigint NOT NULL,
  greatest_modified_value bigint NOT NULL,
  -- table constraints
  CONSTRAINT continuous_aggs_materialization_ranges_materialization_id_fkey FOREIGN KEY (materialization_id) REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_materialization_ranges', '');

CREATE INDEX continuous_aggs_materialization_ranges_idx ON _timescaledb_catalog.continuous_aggs_materialization_ranges (materialization_id, lowest_modified_value ASC);

/* the source of this data is the enum from the source code that lists
 *  the algorithms. This table is NOT dumped.
 */
CREATE TABLE _timescaledb_catalog.compression_algorithm (
  id smallint NOT NULL,
  version smallint NOT NULL,
  name name NOT NULL,
  description text,
  -- table constraints
  CONSTRAINT compression_algorithm_pkey PRIMARY KEY (id)
);

CREATE TABLE _timescaledb_catalog.compression_settings (
  relid regclass NOT NULL,
  compress_relid regclass NULL,
  segmentby text[],
  orderby text[],
  orderby_desc bool[],
  orderby_nullsfirst bool[],
  index jsonb,
  CONSTRAINT compression_settings_pkey PRIMARY KEY (relid),
  CONSTRAINT compression_settings_check_segmentby CHECK (array_ndims(segmentby) = 1),
  CONSTRAINT compression_settings_check_orderby_null CHECK ((orderby IS NULL AND orderby_desc IS NULL AND orderby_nullsfirst IS NULL) OR (orderby IS NOT NULL AND orderby_desc IS NOT NULL AND orderby_nullsfirst IS NOT NULL)),
  CONSTRAINT compression_settings_check_orderby_cardinality CHECK (array_ndims(orderby) = 1 AND array_ndims(orderby_desc) = 1 AND array_ndims(orderby_nullsfirst) = 1 AND cardinality(orderby) = cardinality(orderby_desc) AND cardinality(orderby) = cardinality(orderby_nullsfirst))
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.compression_settings', '');
CREATE INDEX compression_settings_compress_relid_idx ON _timescaledb_catalog.compression_settings (compress_relid);

CREATE TABLE _timescaledb_catalog.compression_chunk_size (
  chunk_id integer NOT NULL,
  compressed_chunk_id integer NOT NULL,
  uncompressed_heap_size bigint NOT NULL,
  uncompressed_toast_size bigint NOT NULL,
  uncompressed_index_size bigint NOT NULL,
  compressed_heap_size bigint NOT NULL,
  compressed_toast_size bigint NOT NULL,
  compressed_index_size bigint NOT NULL,
  numrows_pre_compression bigint,
  numrows_post_compression bigint,
  numrows_frozen_immediately bigint,
  -- table constraints
  CONSTRAINT compression_chunk_size_pkey PRIMARY KEY (chunk_id),
  CONSTRAINT compression_chunk_size_chunk_id_fkey FOREIGN KEY (chunk_id) REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE,
  CONSTRAINT compression_chunk_size_compressed_chunk_id_fkey FOREIGN KEY (compressed_chunk_id) REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE
);

-- Create index on the compressed_chunk_id to speed up maintainance
-- operations during upgrades. This is mostly relevant for very large
-- number of chunks.
CREATE INDEX compression_chunk_size_idx ON _timescaledb_catalog.compression_chunk_size (compressed_chunk_id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.compression_chunk_size', '');

CREATE TABLE _timescaledb_catalog.continuous_agg_migrate_plan (
  mat_hypertable_id integer NOT NULL,
  start_ts TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.now(),
  end_ts TIMESTAMPTZ,
  user_view_definition TEXT,
  -- table constraints
  CONSTRAINT continuous_agg_migrate_plan_pkey PRIMARY KEY (mat_hypertable_id)
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_agg_migrate_plan', '');

CREATE TABLE _timescaledb_catalog.continuous_agg_migrate_plan_step (
  mat_hypertable_id integer NOT NULL,
  step_id serial NOT NULL,
  status TEXT NOT NULL DEFAULT 'NOT STARTED', -- NOT STARTED, STARTED, FINISHED, CANCELED
  start_ts TIMESTAMPTZ,
  end_ts TIMESTAMPTZ,
  type TEXT NOT NULL,
  config JSONB,
  -- table constraints
  CONSTRAINT continuous_agg_migrate_plan_step_pkey PRIMARY KEY (mat_hypertable_id, step_id),
  CONSTRAINT continuous_agg_migrate_plan_step_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.continuous_agg_migrate_plan (mat_hypertable_id) ON DELETE CASCADE,
  CONSTRAINT continuous_agg_migrate_plan_step_check CHECK (start_ts <= end_ts),
  CONSTRAINT continuous_agg_migrate_plan_step_check2 CHECK (type IN ('CREATE NEW CAGG', 'DISABLE POLICIES', 'COPY POLICIES', 'ENABLE POLICIES', 'SAVE WATERMARK', 'REFRESH NEW CAGG', 'COPY DATA', 'OVERRIDE CAGG', 'DROP OLD CAGG'))
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_agg_migrate_plan_step', '');

SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.continuous_agg_migrate_plan_step', 'step_id'), '');

CREATE TABLE _timescaledb_catalog.chunk_rewrite (
  chunk_relid REGCLASS NOT NULL,
  new_relid REGCLASS NOT NULL,
  CONSTRAINT chunk_rewrite_key UNIQUE (chunk_relid)
);

-- Set table permissions
-- We need to grant SELECT to PUBLIC for all tables even those not
-- marked as being dumped because pg_dump will try to access all
-- tables initially to detect inheritance chains and then decide
-- which objects actually need to be dumped.
GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_catalog TO PUBLIC;

GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_internal TO PUBLIC;

GRANT SELECT ON ALL SEQUENCES IN SCHEMA _timescaledb_catalog TO PUBLIC;

GRANT SELECT ON ALL SEQUENCES IN SCHEMA _timescaledb_internal TO PUBLIC;

-- We want to restrict access to the bgw_job_stat_history to only work through
-- the job_errors and job_history views.
REVOKE ALL ON _timescaledb_internal.bgw_job_stat_history FROM PUBLIC;
