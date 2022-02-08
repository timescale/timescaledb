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
  id INTEGER PRIMARY KEY DEFAULT nextval('_timescaledb_catalog.hypertable_id_seq'), 
  schema_name name NOT NULL CHECK (schema_name != '_timescaledb_catalog'),
  table_name name NOT NULL,
  associated_schema_name name NOT NULL,
  associated_table_prefix name NOT NULL,
  num_dimensions smallint NOT NULL,
  chunk_sizing_func_schema name NOT NULL,
  chunk_sizing_func_name name NOT NULL,
  chunk_target_size bigint NOT NULL CHECK (chunk_target_size >= 0), -- size in bytes
  compression_state smallint NOT NULL DEFAULT 0,
  compressed_hypertable_id integer REFERENCES _timescaledb_catalog.hypertable (id),
  replication_factor smallint NULL,
  UNIQUE (associated_schema_name, associated_table_prefix),
  CONSTRAINT hypertable_table_name_schema_name_key UNIQUE (table_name, schema_name),
  -- internal compressed hypertables have compression state = 2
  CONSTRAINT hypertable_dim_compress_check CHECK (num_dimensions > 0 OR compression_state = 2),
  CONSTRAINT hypertable_compress_check CHECK ( (compression_state = 0 OR compression_state = 1 )  OR (compression_state = 2 AND compressed_hypertable_id IS NULL)),
  -- replication_factor NULL: regular hypertable
  -- replication_factor > 0: distributed hypertable on access node
  -- replication_factor -1: distributed hypertable on data node, which is part of a larger table
  CONSTRAINT hypertable_replication_factor_check CHECK (replication_factor > 0 OR replication_factor = -1)
);
ALTER SEQUENCE _timescaledb_catalog.hypertable_id_seq OWNED BY _timescaledb_catalog.hypertable.id;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable_id_seq', '');

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable', '');

CREATE TABLE _timescaledb_catalog.hypertable_data_node (
  hypertable_id integer NOT NULL REFERENCES _timescaledb_catalog.hypertable (id),
  node_hypertable_id integer NULL,
  node_name name NOT NULL,
  block_chunks boolean NOT NULL,
  UNIQUE (node_hypertable_id, node_name),
  UNIQUE (hypertable_id, node_name)
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable_data_node', '');

-- The tablespace table maps tablespaces to hypertables.
-- This allows spreading a hypertable's chunks across multiple disks.
CREATE TABLE _timescaledb_catalog.tablespace (
  id serial PRIMARY KEY,
  hypertable_id int NOT NULL REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  tablespace_name name NOT NULL,
  UNIQUE (hypertable_id, tablespace_name)
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.tablespace', '');

-- A dimension represents an axis along which data is partitioned.
CREATE TABLE _timescaledb_catalog.dimension (
  id serial NOT NULL PRIMARY KEY,
  hypertable_id integer NOT NULL REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  column_name name NOT NULL,
  column_type REGTYPE NOT NULL,
  aligned boolean NOT NULL,
  -- closed dimensions
  num_slices smallint NULL,
  partitioning_func_schema name NULL,
  partitioning_func name NULL,
  -- open dimensions (e.g., time)
  interval_length bigint NULL CHECK (interval_length IS NULL OR interval_length > 0),
  integer_now_func_schema name NULL,
  integer_now_func name NULL,
  CHECK ((partitioning_func_schema IS NULL AND partitioning_func IS NULL) OR (partitioning_func_schema IS NOT NULL AND partitioning_func IS NOT NULL)),
  CHECK ((num_slices IS NULL AND interval_length IS NOT NULL) OR (num_slices IS NOT NULL AND interval_length IS NULL)),
  CHECK ((integer_now_func_schema IS NULL AND integer_now_func IS NULL) OR (integer_now_func_schema IS NOT NULL AND integer_now_func IS NOT NULL)),
  UNIQUE (hypertable_id, column_name)
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.dimension', '');

SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.dimension', 'id'), '');

-- A dimension slice defines a keyspace range along a dimension axis.
CREATE TABLE _timescaledb_catalog.dimension_slice (
  id serial NOT NULL PRIMARY KEY,
  dimension_id integer NOT NULL REFERENCES _timescaledb_catalog.dimension (id) ON DELETE CASCADE,
  range_start bigint NOT NULL,
  range_end bigint NOT NULL,
  CHECK (range_start <= range_end),
  UNIQUE (dimension_id, range_start, range_end)
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
  id integer PRIMARY KEY DEFAULT nextval('_timescaledb_catalog.chunk_id_seq'),
  hypertable_id int NOT NULL REFERENCES _timescaledb_catalog.hypertable (id),
  schema_name name NOT NULL,
  table_name name NOT NULL,
  compressed_chunk_id integer REFERENCES _timescaledb_catalog.chunk (id),
  dropped boolean NOT NULL DEFAULT FALSE,
  status integer NOT NULL DEFAULT 0,
  UNIQUE (schema_name, table_name)
);
ALTER SEQUENCE _timescaledb_catalog.chunk_id_seq OWNED BY _timescaledb_catalog.chunk.id;

CREATE INDEX chunk_hypertable_id_idx ON _timescaledb_catalog.chunk (hypertable_id);

CREATE INDEX chunk_compressed_chunk_id_idx ON _timescaledb_catalog.chunk (compressed_chunk_id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk', '');
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_id_seq', '');

-- A chunk constraint maps a dimension slice to a chunk. Each
-- constraint associated with a chunk will also be a table constraint
-- on the chunk's data table.
CREATE TABLE _timescaledb_catalog.chunk_constraint (
  chunk_id integer NOT NULL REFERENCES _timescaledb_catalog.chunk (id),
  dimension_slice_id integer NULL REFERENCES _timescaledb_catalog.dimension_slice (id),
  constraint_name name NOT NULL,
  hypertable_constraint_name name NULL,
  UNIQUE (chunk_id, constraint_name)
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_constraint', '');

CREATE INDEX chunk_constraint_chunk_id_dimension_slice_id_idx ON _timescaledb_catalog.chunk_constraint (chunk_id, dimension_slice_id);

CREATE SEQUENCE _timescaledb_catalog.chunk_constraint_name;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_constraint_name', '');

CREATE TABLE _timescaledb_catalog.chunk_index (
  chunk_id integer NOT NULL REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE,
  index_name name NOT NULL,
  hypertable_id integer NOT NULL REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  hypertable_index_name name NOT NULL,
  UNIQUE (chunk_id, index_name)
);

CREATE INDEX chunk_index_hypertable_id_hypertable_index_name_idx ON _timescaledb_catalog.chunk_index (hypertable_id, hypertable_index_name);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_index', '');

CREATE TABLE _timescaledb_catalog.chunk_data_node (
  chunk_id integer NOT NULL REFERENCES _timescaledb_catalog.chunk (id),
  node_chunk_id integer NOT NULL,
  node_name name NOT NULL,
  UNIQUE (node_chunk_id, node_name),
  UNIQUE (chunk_id, node_name)
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.chunk_data_node', '');

-- Default jobs are given the id space [1,1000). User-installed jobs and any jobs created inside tests
-- are given the id space [1000, INT_MAX). That way, we do not pg_dump jobs that are always default-installed
-- inside other .sql scripts. This avoids insertion conflicts during pg_restore.
CREATE SEQUENCE _timescaledb_config.bgw_job_id_seq
MINVALUE 1000;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_job_id_seq', '');

CREATE TABLE _timescaledb_config.bgw_job (
  id integer PRIMARY KEY DEFAULT nextval('_timescaledb_config.bgw_job_id_seq'),
  application_name name NOT NULL,
  schedule_interval interval NOT NULL,
  max_runtime interval NOT NULL,
  max_retries integer NOT NULL,
  retry_period interval NOT NULL,
  proc_schema name NOT NULL,
  proc_name name NOT NULL,
  owner name NOT NULL DEFAULT CURRENT_ROLE,
  scheduled bool NOT NULL DEFAULT TRUE,
  hypertable_id integer REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  config jsonb
);

ALTER SEQUENCE _timescaledb_config.bgw_job_id_seq OWNED BY _timescaledb_config.bgw_job.id;

CREATE INDEX bgw_job_proc_hypertable_id_idx ON _timescaledb_config.bgw_job (proc_schema, proc_name, hypertable_id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_job', 'WHERE id >= 1000');

CREATE TABLE _timescaledb_internal.bgw_job_stat (
  job_id integer PRIMARY KEY REFERENCES _timescaledb_config.bgw_job (id) ON DELETE CASCADE,
  last_start timestamptz NOT NULL DEFAULT NOW(),
  last_finish timestamptz NOT NULL,
  next_start timestamptz NOT NULL,
  last_successful_finish timestamptz NOT NULL,
  last_run_success bool NOT NULL,
  total_runs bigint NOT NULL,
  total_duration interval NOT NULL,
  total_successes bigint NOT NULL,
  total_failures bigint NOT NULL,
  total_crashes bigint NOT NULL,
  consecutive_failures int NOT NULL,
  consecutive_crashes int NOT NULL
);

--The job_stat table is not dumped by pg_dump on purpose because
--the statistics probably aren't very meaningful across instances.
-- Now we define a special stats table for each job/chunk pair. This will be used by the scheduler
-- to determine whether to run a specific job on a specific chunk.
CREATE TABLE _timescaledb_internal.bgw_policy_chunk_stats (
  job_id integer NOT NULL REFERENCES _timescaledb_config.bgw_job (id) ON DELETE CASCADE,
  chunk_id integer NOT NULL REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE,
  num_times_job_run integer,
  last_time_job_run timestamptz,
  UNIQUE (job_id, chunk_id)
);

CREATE TABLE _timescaledb_catalog.metadata (
  key NAME NOT NULL PRIMARY KEY,
  value text NOT NULL,
  include_in_telemetry boolean NOT NULL
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.metadata', $$
  WHERE KEY = 'exported_uuid' $$);

CREATE TABLE _timescaledb_catalog.continuous_agg (
  mat_hypertable_id integer PRIMARY KEY REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  raw_hypertable_id integer NOT NULL REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  user_view_schema name NOT NULL,
  user_view_name name NOT NULL,
  partial_view_schema name NOT NULL,
  partial_view_name name NOT NULL,
  bucket_width bigint NOT NULL,
  direct_view_schema name NOT NULL,
  direct_view_name name NOT NULL,
  materialized_only bool NOT NULL DEFAULT FALSE,
  UNIQUE (user_view_schema, user_view_name),
  UNIQUE (partial_view_schema, partial_view_name)
);

CREATE INDEX continuous_agg_raw_hypertable_id_idx ON _timescaledb_catalog.continuous_agg (raw_hypertable_id);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_agg', '');

-- See the comments for ContinuousAggsBucketFunction structure.
CREATE TABLE _timescaledb_catalog.continuous_aggs_bucket_function(
  mat_hypertable_id integer PRIMARY KEY REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  -- The schema of the function. Equals TRUE for "timescaledb_experimental", FALSE otherwise.
  experimental bool NOT NULL,
  -- Name of the bucketing function, e.g. "time_bucket" or "time_bucket_ng"
  name text NOT NULL,
  -- `bucket_width` argument of the function, e.g. "1 month"
  bucket_width text NOT NULL,
  -- `origin` argument of the function provided by the user
  origin text NOT NULL,
  -- `timezone` argument of the function provided by the user
  timezone text NOT NULL
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_bucket_function', '');

CREATE TABLE _timescaledb_catalog.continuous_aggs_invalidation_threshold (
  hypertable_id integer PRIMARY KEY REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  watermark bigint NOT NULL
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_invalidation_threshold', '');

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
  materialization_id integer REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE,
  lowest_modified_value bigint NOT NULL,
  greatest_modified_value bigint NOT NULL
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_materialization_invalidation_log', '');

CREATE INDEX continuous_aggs_materialization_invalidation_log_idx ON _timescaledb_catalog.continuous_aggs_materialization_invalidation_log (materialization_id, lowest_modified_value ASC);


/* the source of this data is the enum from the source code that lists
 *  the algorithms. This table is NOT dumped.
 */
CREATE TABLE _timescaledb_catalog.compression_algorithm (
  id smallint PRIMARY KEY,
  version smallint NOT NULL,
  name name NOT NULL,
  description text
);

CREATE TABLE _timescaledb_catalog.hypertable_compression (
  hypertable_id integer REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  attname name NOT NULL,
  compression_algorithm_id smallint REFERENCES _timescaledb_catalog.compression_algorithm (id),
  segmentby_column_index smallint,
  orderby_column_index smallint,
  orderby_asc boolean,
  orderby_nullsfirst boolean,
  PRIMARY KEY (hypertable_id, attname),
  UNIQUE (hypertable_id, segmentby_column_index),
  UNIQUE (hypertable_id, orderby_column_index)
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable_compression', '');

CREATE TABLE _timescaledb_catalog.compression_chunk_size (
  chunk_id integer REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE,
  compressed_chunk_id integer REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE,
  uncompressed_heap_size bigint NOT NULL,
  uncompressed_toast_size bigint NOT NULL,
  uncompressed_index_size bigint NOT NULL,
  compressed_heap_size bigint NOT NULL,
  compressed_toast_size bigint NOT NULL,
  compressed_index_size bigint NOT NULL,
  numrows_pre_compression bigint,
  numrows_post_compression bigint,
  PRIMARY KEY (chunk_id, compressed_chunk_id)
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.compression_chunk_size', '');

--This stores commit decisions for 2pc remote txns. Abort decisions are never stored.
--If a PREPARE TRANSACTION fails for any data node then the entire
--frontend transaction will be rolled back and no rows will be stored.
--the frontend_transaction_id represents the entire distributed transaction
--each datanode will have a unique remote_transaction_id.
CREATE TABLE _timescaledb_catalog.remote_txn (
  data_node_name name, --this is really only to allow us to cleanup stuff on a per-node basis.
  remote_transaction_id text CHECK (remote_transaction_id::@extschema@.rxid IS NOT NULL),
  PRIMARY KEY (remote_transaction_id)
);

CREATE INDEX remote_txn_data_node_name_idx ON _timescaledb_catalog.remote_txn (data_node_name);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.remote_txn', '');

-- This table stores information about the stage that has been completed of a
-- chunk move/copy activity
--
-- A cleanup activity can query and check if the backend is running. If the
-- backend has exited then we can commence cleanup. The cleanup
-- activity can also do a diff with the "time_start" value to ascertain if
-- the entire end-to-end activity is going on for too long
--
-- We also track the end time of every stage. A diff with the current time
-- will give us an idea about how long the current stage has been running
--
-- Entry for a chunk move/copy activity gets deleted on successful completion
--
-- We don't want to pg_dump this table's contents. A node restored using it
-- could be part of a totally different multinode setup and we don't want to
-- carry over chunk copy/move operations from earlier (if it makes sense at all)
--

CREATE SEQUENCE _timescaledb_catalog.chunk_copy_operation_id_seq MINVALUE 1;

CREATE TABLE _timescaledb_catalog.chunk_copy_operation (
  operation_id name PRIMARY KEY, -- the publisher/subscriber identifier used
  backend_pid integer NOT NULL, -- the pid of the backend running this activity
  completed_stage name NOT NULL, -- the completed stage/step
  time_start timestamptz NOT NULL DEFAULT NOW(), -- start time of the activity
  chunk_id integer NOT NULL REFERENCES _timescaledb_catalog.chunk (id) ON DELETE CASCADE,
  source_node_name name NOT NULL,
  dest_node_name name NOT NULL,
  delete_on_source_node bool NOT NULL -- is a move or copy activity
);

-- Set table permissions
-- We need to grant SELECT to PUBLIC for all tables even those not
-- marked as being dumped because pg_dump will try to access all
-- tables initially to detect inheritance chains and then decide
-- which objects actually need to be dumped.
GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_catalog TO PUBLIC;

GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_config TO PUBLIC;

GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_internal TO PUBLIC;

GRANT SELECT ON ALL SEQUENCES IN SCHEMA _timescaledb_catalog TO PUBLIC;

GRANT SELECT ON ALL SEQUENCES IN SCHEMA _timescaledb_config TO PUBLIC;

GRANT SELECT ON ALL SEQUENCES IN SCHEMA _timescaledb_internal TO PUBLIC;
