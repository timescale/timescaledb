DROP FUNCTION IF EXISTS drop_chunks("any",name,name,boolean,"any",boolean,boolean);
DROP FUNCTION IF EXISTS add_drop_chunks_policy(REGCLASS,INTERVAL,BOOL,BOOL,BOOL);

ALTER TABLE _timescaledb_catalog.dimension
ADD COLUMN integer_now_func_schema     NAME     NULL;

ALTER TABLE _timescaledb_catalog.dimension
ADD COLUMN integer_now_func            NAME     NULL;

ALTER TABLE _timescaledb_catalog.dimension
ADD CONSTRAINT dimension_check2
CHECK (
        (integer_now_func_schema IS NULL AND integer_now_func IS NULL) OR
        (integer_now_func_schema IS NOT NULL AND integer_now_func IS NOT NULL)
);
-- ----------------------
CREATE TYPE _timescaledb_catalog.ts_interval AS (
    is_time_interval        BOOLEAN,
    time_interval		    INTERVAL,
    integer_interval        BIGINT
    );

-- q -- todo:: this is probably necessary if we keep the validation constraint in the table definition.
CREATE OR REPLACE FUNCTION _timescaledb_internal.valid_ts_interval(invl _timescaledb_catalog.ts_interval)
RETURNS BOOLEAN AS '@MODULE_PATHNAME@', 'ts_valid_ts_interval' LANGUAGE C VOLATILE STRICT;

DROP VIEW IF EXISTS timescaledb_information.drop_chunks_policies;
DROP VIEW IF EXISTS timescaledb_information.policy_stats;

CREATE TABLE IF NOT EXISTS _timescaledb_config.bgw_policy_drop_chunks_tmp (
    job_id          		    INTEGER                 PRIMARY KEY REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE,
    hypertable_id   		    INTEGER     UNIQUE      NOT NULL REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    older_than	    _timescaledb_catalog.ts_interval    NOT NULL,
	cascade					    BOOLEAN                 NOT NULL,
    cascade_to_materializations BOOLEAN                 NOT NULL,
    CONSTRAINT valid_older_than CHECK(_timescaledb_internal.valid_ts_interval(older_than))
);

INSERT INTO _timescaledb_config.bgw_policy_drop_chunks_tmp
(SELECT job_id, hypertable_id, ROW('t',older_than,NULL)::_timescaledb_catalog.ts_interval  as older_than, cascade, cascade_to_materializations
FROM _timescaledb_config.bgw_policy_drop_chunks);

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_config.bgw_policy_drop_chunks;
DROP TABLE _timescaledb_config.bgw_policy_drop_chunks;

CREATE TABLE IF NOT EXISTS _timescaledb_config.bgw_policy_drop_chunks (
    job_id          		    INTEGER                 PRIMARY KEY REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE,
    hypertable_id   		    INTEGER     UNIQUE      NOT NULL REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
    older_than	    _timescaledb_catalog.ts_interval    NOT NULL,
	cascade					    BOOLEAN                 NOT NULL,
    cascade_to_materializations BOOLEAN                 NOT NULL,
    CONSTRAINT valid_older_than CHECK(_timescaledb_internal.valid_ts_interval(older_than))
);

INSERT INTO _timescaledb_config.bgw_policy_drop_chunks
(SELECT * FROM _timescaledb_config.bgw_policy_drop_chunks_tmp);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_policy_drop_chunks', '');
DROP TABLE _timescaledb_config.bgw_policy_drop_chunks_tmp;
GRANT SELECT ON _timescaledb_config.bgw_policy_drop_chunks TO PUBLIC;

DROP FUNCTION IF EXISTS alter_job_schedule(INTEGER, INTERVAL, INTERVAL, INTEGER, INTERVAL, BOOL);


--ADDS last_successful_finish column
--Must remove from extension first
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_internal.bgw_job_stat;
DROP VIEW IF EXISTS timescaledb_information.policy_stats;
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregate_stats;

--create table and drop instead of rename so that all indexes dropped as well
CREATE TABLE _timescaledb_internal.bgw_job_stat_tmp AS SELECT * FROM _timescaledb_internal.bgw_job_stat;
DROP TABLE _timescaledb_internal.bgw_job_stat;

CREATE TABLE IF NOT EXISTS _timescaledb_internal.bgw_job_stat (
    job_id                  INTEGER         PRIMARY KEY REFERENCES _timescaledb_config.bgw_job(id) ON DELETE CASCADE,
    last_start              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_finish             TIMESTAMPTZ NOT NULL,
    next_start              TIMESTAMPTZ NOT NULL,
    last_successful_finish  TIMESTAMPTZ NOT NULL,
    last_run_success        BOOL        NOT NULL,
    total_runs              BIGINT      NOT NULL,
    total_duration          INTERVAL    NOT NULL,
    total_successes         BIGINT      NOT NULL,
    total_failures          BIGINT      NOT NULL,
    total_crashes           BIGINT      NOT NULL,
    consecutive_failures    INT         NOT NULL,
    consecutive_crashes     INT         NOT NULL
);
--The job_stat table is not dumped by pg_dump on purpose because (see tables.sql for details)

INSERT INTO _timescaledb_internal.bgw_job_stat
    SELECT job_id,
        last_start,
        last_finish,
        next_start,
        CASE WHEN last_run_success THEN last_finish ELSE '-infinity'::timestamptz END as last_successful_finish,
        last_run_success,
        total_runs,
        total_duration,
        total_successes,
        total_failures,
        total_crashes,
        consecutive_failures,
        consecutive_crashes
    FROM _timescaledb_internal.bgw_job_stat_tmp;

DROP TABLE _timescaledb_internal.bgw_job_stat_tmp;
GRANT SELECT ON _timescaledb_internal.bgw_job_stat TO PUBLIC;





ALTER TABLE _timescaledb_catalog.hypertable add column compressed boolean NOT NULL default false;
ALTER TABLE _timescaledb_catalog.hypertable add column compressed_hypertable_id          INTEGER   REFERENCES _timescaledb_catalog.hypertable(id);
ALTER TABLE _timescaledb_catalog.hypertable drop constraint hypertable_num_dimensions_check;
ALTER TABLE _timescaledb_catalog.hypertable add constraint hypertable_dim_compress_check check ( num_dimensions > 0  or compressed = true );
alter table _timescaledb_catalog.hypertable add constraint hypertable_compress_check check ( compressed = false or (compressed = true and compressed_hypertable_id is null ));

ALTER TABLE _timescaledb_catalog.chunk add column compressed_chunk_id integer references _timescaledb_catalog.chunk(id);

CREATE TABLE _timescaledb_catalog.compression_algorithm(
	id SMALLINT PRIMARY KEY,
	version SMALLINT NOT NULL,
	name NAME NOT NULL,
	description TEXT
);

CREATE TABLE IF NOT EXISTS _timescaledb_catalog.hypertable_compression (
	hypertable_id INTEGER REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
	attname NAME NOT NULL,
	compression_algorithm_id SMALLINT REFERENCES _timescaledb_catalog.compression_algorithm(id),
    segmentby_column_index SMALLINT ,
    orderby_column_index SMALLINT,
    orderby_asc BOOLEAN,
    orderby_nullsfirst BOOLEAN,
	PRIMARY KEY (hypertable_id, attname),
    UNIQUE (hypertable_id, segmentby_column_index),
    UNIQUE (hypertable_id, orderby_column_index)
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.hypertable_compression', '');

CREATE TABLE IF NOT EXISTS _timescaledb_catalog.compression_chunk_size (

    chunk_id            INTEGER REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE,
    compressed_chunk_id   INTEGER REFERENCES _timescaledb_catalog.chunk(id) ON DELETE CASCADE,
    uncompressed_heap_size BIGINT NOT NULL,
    uncompressed_toast_size BIGINT NOT NULL,
    uncompressed_index_size BIGINT NOT NULL,
    compressed_heap_size BIGINT NOT NULL,
    compressed_toast_size BIGINT NOT NULL,
    compressed_index_size BIGINT NOT NULL,
    PRIMARY KEY( chunk_id, compressed_chunk_id)
);
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.compression_chunk_size', '');

CREATE TABLE IF NOT EXISTS _timescaledb_config.bgw_compress_chunks_policy(
	hypertable_id INTEGER REFERENCES _timescaledb_catalog.hypertable(id) ON DELETE CASCADE,
	older_than BIGINT NOT NULL,
	job_id SMALLINT  REFERENCES _timescaledb_config.bgw_job(id),
	UNIQUE (hypertable_id, job_id)
);

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_config.bgw_compress_chunks_policy', '');


GRANT SELECT ON _timescaledb_catalog.compression_algorithm TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.hypertable_compression TO PUBLIC;
GRANT SELECT ON _timescaledb_catalog.compression_chunk_size TO PUBLIC;
GRANT SELECT ON _timescaledb_config.bgw_compress_chunks_policy TO PUBLIC;

CREATE TYPE _timescaledb_internal.compressed_data;

--the textual input/output is simply base64 encoding of the binary representation
CREATE FUNCTION _timescaledb_internal.compressed_data_in(CSTRING)
   RETURNS _timescaledb_internal.compressed_data
   AS '@MODULE_PATHNAME@', 'ts_compressed_data_in'
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION _timescaledb_internal.compressed_data_out(_timescaledb_internal.compressed_data)
   RETURNS CSTRING
   AS '@MODULE_PATHNAME@', 'ts_compressed_data_out'
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION _timescaledb_internal.compressed_data_send(_timescaledb_internal.compressed_data)
   RETURNS BYTEA
   AS '@MODULE_PATHNAME@', 'ts_compressed_data_send'
   LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION _timescaledb_internal.compressed_data_recv(internal)
   RETURNS _timescaledb_internal.compressed_data
   AS '@MODULE_PATHNAME@', 'ts_compressed_data_recv'
   LANGUAGE C IMMUTABLE STRICT;

CREATE TYPE _timescaledb_internal.compressed_data (
    INTERNALLENGTH = VARIABLE,
    STORAGE = EXTERNAL,
    ALIGNMENT = DOUBLE, --needed for alignment in ARRAY type compression
    INPUT = _timescaledb_internal.compressed_data_in,
    OUTPUT = _timescaledb_internal.compressed_data_out,
    RECEIVE = _timescaledb_internal.compressed_data_recv,
    SEND = _timescaledb_internal.compressed_data_send
);

--insert data for compression_algorithm --
insert into _timescaledb_catalog.compression_algorithm values
( 0, 1, 'COMPRESSION_ALGORITHM_NONE', 'no compression'),
( 1, 1, 'COMPRESSION_ALGORITHM_ARRAY', 'array'),
( 2, 1, 'COMPRESSION_ALGORITHM_DICTIONARY', 'dictionary'),
( 3, 1, 'COMPRESSION_ALGORITHM_GORILLA', 'gorilla'),
( 4, 1, 'COMPRESSION_ALGORITHM_DELTADELTA', 'deltadelta')
on conflict(id) do update set (version, name, description) 
= (excluded.version, excluded.name, excluded.description);
