DROP FUNCTION _timescaledb_internal.ping_data_node(NAME);

CREATE FUNCTION _timescaledb_internal.ping_data_node(node_name NAME, timeout INTERVAL = NULL) RETURNS BOOLEAN
AS '@MODULE_PATHNAME@', 'ts_data_node_ping' LANGUAGE C VOLATILE;

CREATE TABLE _timescaledb_catalog.continuous_aggs_watermark (
  mat_hypertable_id integer NOT NULL,
  watermark bigint NOT NULL,
  -- table constraints
  CONSTRAINT continuous_aggs_watermark_pkey PRIMARY KEY (mat_hypertable_id),
  CONSTRAINT continuous_aggs_watermark_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE
);

GRANT SELECT ON _timescaledb_catalog.continuous_aggs_watermark TO PUBLIC;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_watermark', '');

CREATE FUNCTION _timescaledb_internal.cagg_watermark_materialized(hypertable_id INTEGER)
RETURNS INT8 AS '@MODULE_PATHNAME@', 'ts_continuous_agg_watermark_materialized' LANGUAGE C STABLE STRICT PARALLEL SAFE;
CREATE FUNCTION _timescaledb_internal.recompress_chunk_segmentwise(REGCLASS, BOOLEAN) RETURNS REGCLASS
AS '@MODULE_PATHNAME@', 'ts_recompress_chunk_segmentwise' LANGUAGE C STRICT VOLATILE;
CREATE FUNCTION _timescaledb_internal.get_compressed_chunk_index_for_recompression(REGCLASS) RETURNS REGCLASS
AS '@MODULE_PATHNAME@', 'ts_get_compressed_chunk_index_for_recompression' LANGUAGE C STRICT VOLATILE;

DROP FUNCTION _timescaledb_internal.dimension_is_finite;
DROP FUNCTION _timescaledb_internal.dimension_slice_get_constraint_sql;

CREATE SCHEMA _timescaledb_functions;
GRANT USAGE ON SCHEMA _timescaledb_functions TO PUBLIC;

-- migrate histogram support functions into _timescaledb_functions schema
ALTER FUNCTION _timescaledb_internal.hist_sfunc (state INTERNAL, val DOUBLE PRECISION, MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.hist_combinefunc(state1 INTERNAL, state2 INTERNAL) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.hist_serializefunc(INTERNAL) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.hist_deserializefunc(bytea, INTERNAL) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.hist_finalfunc(state INTERNAL, val DOUBLE PRECISION, MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER) SET SCHEMA _timescaledb_functions;

-- migrate first/last support functions into _timescaledb_functions schema
ALTER FUNCTION _timescaledb_internal.first_sfunc(internal, anyelement, "any") SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.first_combinefunc(internal, internal) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.last_sfunc(internal, anyelement, "any") SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.last_combinefunc(internal, internal) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.bookend_finalfunc(internal, anyelement, "any") SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.bookend_serializefunc(internal) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.bookend_deserializefunc(bytea, internal) SET SCHEMA _timescaledb_functions;

DROP FUNCTION IF EXISTS _timescaledb_internal.is_main_table(regclass);
DROP FUNCTION IF EXISTS _timescaledb_internal.is_main_table(name, name);
DROP FUNCTION IF EXISTS _timescaledb_internal.hypertable_from_main_table(regclass);
DROP FUNCTION IF EXISTS _timescaledb_internal.main_table_from_hypertable(integer);
DROP FUNCTION IF EXISTS _timescaledb_internal.time_literal_sql(bigint, regtype);

ALTER FUNCTION _timescaledb_internal.compressed_data_in(CSTRING) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.compressed_data_out(_timescaledb_internal.compressed_data) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.compressed_data_send(_timescaledb_internal.compressed_data) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.compressed_data_recv(internal) SET SCHEMA _timescaledb_functions;

ALTER FUNCTION _timescaledb_internal.rxid_in(cstring) SET SCHEMA _timescaledb_functions;
ALTER FUNCTION _timescaledb_internal.rxid_out(@extschema@.rxid) SET SCHEMA _timescaledb_functions;

-- drop dependent views
DROP VIEW IF EXISTS timescaledb_information.job_errors;
DROP VIEW IF EXISTS timescaledb_information.job_stats;
DROP VIEW IF EXISTS timescaledb_information.jobs;
DROP VIEW IF EXISTS timescaledb_experimental.policies;

ALTER TABLE _timescaledb_config.bgw_job
    ALTER COLUMN owner DROP DEFAULT,
    ALTER COLUMN owner TYPE regrole USING owner::regrole,
    ALTER COLUMN owner SET DEFAULT current_role::regrole;
CREATE TABLE _timescaledb_config.bgw_job_tmp AS SELECT * FROM _timescaledb_config.bgw_job;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_config.bgw_job;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_config.bgw_job_id_seq;
ALTER TABLE _timescaledb_internal.bgw_job_stat
      DROP CONSTRAINT IF EXISTS bgw_job_stat_job_id_fkey;
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats
      DROP CONSTRAINT IF EXISTS bgw_policy_chunk_stats_job_id_fkey;
CREATE TABLE _timescaledb_internal.tmp_bgw_job_seq_value AS
       SELECT last_value, is_called FROM _timescaledb_config.bgw_job_id_seq;
DROP TABLE _timescaledb_config.bgw_job;

CREATE SEQUENCE _timescaledb_config.bgw_job_id_seq MINVALUE 1000;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_job_id_seq', '');
SELECT pg_catalog.setval('_timescaledb_config.bgw_job_id_seq', last_value, is_called)
  FROM _timescaledb_internal.tmp_bgw_job_seq_value;
DROP TABLE _timescaledb_internal.tmp_bgw_job_seq_value;

CREATE TABLE _timescaledb_config.bgw_job (
  id integer NOT NULL DEFAULT nextval('_timescaledb_config.bgw_job_id_seq'),
  application_name name NOT NULL,
  schedule_interval interval NOT NULL,
  max_runtime interval NOT NULL,
  max_retries integer NOT NULL,
  retry_period interval NOT NULL,
  proc_schema name NOT NULL,
  proc_name name NOT NULL,
  owner regrole NOT NULL DEFAULT current_role::regrole,
  scheduled bool NOT NULL DEFAULT TRUE,
  fixed_schedule bool not null default true,
  initial_start timestamptz,
  hypertable_id integer,
  config jsonb,
  check_schema name,
  check_name name,
  timezone text,
  CONSTRAINT bgw_job_pkey PRIMARY KEY (id),
  CONSTRAINT bgw_job_hypertable_id_fkey FOREIGN KEY (hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE
);

ALTER SEQUENCE _timescaledb_config.bgw_job_id_seq OWNED BY _timescaledb_config.bgw_job.id;
CREATE INDEX bgw_job_proc_hypertable_id_idx
       ON _timescaledb_config.bgw_job(proc_schema,proc_name,hypertable_id);
INSERT INTO _timescaledb_config.bgw_job
       SELECT * FROM _timescaledb_config.bgw_job_tmp ORDER BY id;
DROP TABLE _timescaledb_config.bgw_job_tmp;
ALTER TABLE _timescaledb_internal.bgw_job_stat
      ADD CONSTRAINT bgw_job_stat_job_id_fkey
      	  FOREIGN KEY(job_id)
	  REFERENCES _timescaledb_config.bgw_job(id)
	  ON DELETE CASCADE;
ALTER TABLE _timescaledb_internal.bgw_policy_chunk_stats
      ADD CONSTRAINT bgw_policy_chunk_stats_job_id_fkey
      	  FOREIGN KEY(job_id)
	  REFERENCES _timescaledb_config.bgw_job(id)
	  ON DELETE CASCADE;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_job', 'WHERE id >= 1000');
GRANT SELECT ON _timescaledb_config.bgw_job TO PUBLIC;
GRANT SELECT ON _timescaledb_config.bgw_job_id_seq TO PUBLIC;
