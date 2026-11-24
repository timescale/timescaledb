DROP FUNCTION IF EXISTS _timescaledb_functions.get_segmentby_defaults(regclass);
DROP FUNCTION IF EXISTS _timescaledb_functions.get_orderby_defaults(regclass, text[]);

DROP VIEW IF EXISTS timescaledb_information.job_history;

CREATE TABLE _timescaledb_catalog.continuous_aggs_bucket_function (
  mat_hypertable_id integer NOT NULL,
  bucket_func text NOT NULL,
  bucket_width text NOT NULL,
  bucket_origin text,
  bucket_offset text,
  bucket_timezone text,
  bucket_fixed_width bool NOT NULL,
  CONSTRAINT continuous_aggs_bucket_function_pkey PRIMARY KEY (mat_hypertable_id),
  CONSTRAINT continuous_aggs_bucket_function_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.hypertable (id) ON DELETE CASCADE,
  CONSTRAINT continuous_aggs_bucket_function_func_check CHECK (pg_catalog.to_regprocedure(bucket_func) IS DISTINCT FROM 0)
);

-- Use the function of the version we downgrade to for filling the table
CREATE OR REPLACE FUNCTION _timescaledb_functions.cagg_get_bucket_function_info(
    mat_hypertable_id INTEGER,
    OUT bucket_func REGPROCEDURE,
    OUT bucket_width TEXT,
    OUT bucket_origin TEXT,
    OUT bucket_offset TEXT,
    OUT bucket_timezone TEXT,
    OUT bucket_fixed_width BOOLEAN
) RETURNS RECORD AS '@MODULE_PATHNAME@', 'ts_continuous_agg_get_bucket_function_info' LANGUAGE C STRICT VOLATILE;

-- Use fixed output formats and time zone so that the stored values do
-- not depend on the settings of the session running the downgrade.
SET LOCAL datestyle TO 'ISO, YMD';
SET LOCAL intervalstyle TO 'postgres';
SET LOCAL timezone TO 'UTC';

INSERT INTO _timescaledb_catalog.continuous_aggs_bucket_function
  SELECT
    ca.mat_hypertable_id,
    (pg_catalog.pg_identify_object('pg_catalog.pg_proc'::pg_catalog.regclass, bf.bucket_func::pg_catalog.oid, 0)).identity,
    bf.bucket_width,
    bf.bucket_origin::pg_catalog.timestamptz::pg_catalog.text,
    bf.bucket_offset,
    bf.bucket_timezone,
    bf.bucket_fixed_width
  FROM _timescaledb_catalog.continuous_agg ca,
    _timescaledb_functions.cagg_get_bucket_function_info(ca.mat_hypertable_id) bf;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_aggs_bucket_function', '');

GRANT SELECT ON TABLE _timescaledb_catalog.continuous_aggs_bucket_function TO PUBLIC;

ALTER TABLE _timescaledb_catalog.chunk DROP CONSTRAINT chunk_check;
DROP FUNCTION _timescaledb_functions.chunk_validate(regclass, integer);

ALTER TABLE _timescaledb_catalog.compression_settings DROP CONSTRAINT compression_settings_check_relid;
DROP FUNCTION _timescaledb_functions.compression_settings_validate(regclass, regclass);

ALTER TABLE _timescaledb_catalog.chunk_rewrite DROP CONSTRAINT chunk_rewrite_check;
DROP FUNCTION _timescaledb_functions.chunk_rewrite_validate(regclass, regclass);

ALTER TABLE _timescaledb_catalog.hypertable DROP CONSTRAINT hypertable_check;
DROP FUNCTION _timescaledb_functions.hypertable_validate(integer, name, name, name);

DROP FUNCTION _timescaledb_functions.relation_validate(oid, "char"[], text);
DROP FUNCTION _timescaledb_functions.owner_validate(oid, text);
