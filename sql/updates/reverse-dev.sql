DROP PROCEDURE _timescaledb_functions.process_hypertable_invalidations(REGCLASS);
DROP PROCEDURE @extschema@.add_process_hypertable_invalidations_policy(REGCLASS, INTERVAL, BOOL, TIMESTAMPTZ, TEXT);
DROP PROCEDURE @extschema@.remove_process_hypertable_invalidations_policy(REGCLASS, BOOL);
DROP PROCEDURE _timescaledb_functions.policy_process_hypertable_invalidations(INTEGER, JSONB);
DROP FUNCTION _timescaledb_functions.policy_process_hypertable_invalidations_check(JSONB);

DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression(job_id INTEGER, config JSONB);
DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression_execute(
  INTEGER, INTEGER, ANYELEMENT, INTEGER, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN, BOOLEAN
);

CREATE PROCEDURE _timescaledb_functions.policy_compression_execute(
  job_id              INTEGER,
  htid                INTEGER,
  lag                 ANYELEMENT,
  maxchunks           INTEGER,
  verbose_log         BOOLEAN,
  recompress_enabled  BOOLEAN,
  use_creation_time   BOOLEAN,
  useam               BOOLEAN = NULL)
AS $$
BEGIN
  -- empty body
END;
$$ LANGUAGE PLPGSQL;

DROP PROCEDURE @extschema@.refresh_continuous_aggregate(
    continuous_aggregate REGCLASS,
    window_start "any",
    window_end "any",
    force BOOLEAN,
    options JSONB
);

CREATE PROCEDURE @extschema@.refresh_continuous_aggregate(
    continuous_aggregate     REGCLASS,
    window_start             "any",
    window_end               "any",
    force                    BOOLEAN = FALSE
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_update_placeholder';

DROP PROCEDURE IF EXISTS @extschema@.detach_chunk(REGCLASS);
DROP PROCEDURE IF EXISTS @extschema@.attach_chunk(REGCLASS, REGCLASS, JSONB);

-- downgrade compression settings
CREATE TABLE _timescaledb_catalog.tempsettings (LIKE _timescaledb_catalog.compression_settings);
INSERT INTO _timescaledb_catalog.tempsettings SELECT * FROM _timescaledb_catalog.compression_settings;
DROP VIEW timescaledb_information.hypertable_columnstore_settings;
DROP VIEW timescaledb_information.chunk_columnstore_settings;
DROP VIEW timescaledb_information.hypertable_compression_settings;
DROP VIEW timescaledb_information.chunk_compression_settings;
DROP VIEW timescaledb_information.compression_settings;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.compression_settings;
DROP TABLE _timescaledb_catalog.compression_settings;

CREATE TABLE _timescaledb_catalog.compression_settings (
  relid regclass NOT NULL,
  compress_relid regclass NULL,
  segmentby text[],
  orderby text[],
  orderby_desc bool[],
  orderby_nullsfirst bool[],
  CONSTRAINT compression_settings_pkey PRIMARY KEY (relid),
  CONSTRAINT compression_settings_check_segmentby CHECK (array_ndims(segmentby) = 1),
  CONSTRAINT compression_settings_check_orderby_null CHECK ((orderby IS NULL AND orderby_desc IS NULL AND orderby_nullsfirst IS NULL) OR (orderby IS NOT NULL AND orderby_desc IS NOT NULL AND orderby_nullsfirst IS NOT NULL)),
  CONSTRAINT compression_settings_check_orderby_cardinality CHECK (array_ndims(orderby) = 1 AND array_ndims(orderby_desc) = 1 AND array_ndims(orderby_nullsfirst) = 1 AND cardinality(orderby) = cardinality(orderby_desc) AND cardinality(orderby) = cardinality(orderby_nullsfirst))
);

-- Revert information in compression settings
INSERT INTO _timescaledb_catalog.compression_settings
SELECT
    cs.relid,
    cs.compress_relid,
    cs.segmentby,
    cs.orderby,
    cs.orderby_desc,
    cs.orderby_nullsfirst
FROM
    _timescaledb_catalog.tempsettings cs;

DROP TABLE _timescaledb_catalog.tempsettings CASCADE;

CREATE INDEX compression_settings_compress_relid_idx ON _timescaledb_catalog.compression_settings (compress_relid);

GRANT SELECT ON _timescaledb_catalog.compression_settings TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.compression_settings', '');

DROP FUNCTION IF EXISTS _timescaledb_functions.jsonb_get_matching_index_entry(jsonb, text, text);
