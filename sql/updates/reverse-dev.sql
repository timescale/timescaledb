ALTER EXTENSION timescaledb DROP VIEW timescaledb_information.continuous_aggregates;

DROP VIEW timescaledb_information.continuous_aggregates;

DROP PROCEDURE _timescaledb_functions.process_hypertable_invalidations(REGCLASS[]);
DROP PROCEDURE _timescaledb_functions.process_hypertable_invalidations(NAME);
DROP FUNCTION _timescaledb_functions.cagg_parse_invalidation_record(BYTEA);
DROP FUNCTION _timescaledb_functions.has_invalidation_trigger(regclass);

CREATE FUNCTION ts_hypercore_handler(internal) RETURNS table_am_handler
AS '@MODULE_PATHNAME@', 'ts_hypercore_handler' LANGUAGE C;

CREATE FUNCTION ts_hypercore_proxy_handler(internal) RETURNS index_am_handler
AS '@MODULE_PATHNAME@', 'ts_hypercore_proxy_handler' LANGUAGE C;

CREATE ACCESS METHOD hypercore TYPE TABLE HANDLER ts_hypercore_handler;
COMMENT ON ACCESS METHOD hypercore IS 'Storage engine using hybrid row/columnar compression';

CREATE ACCESS METHOD hypercore_proxy TYPE INDEX HANDLER ts_hypercore_proxy_handler;
COMMENT ON ACCESS METHOD hypercore_proxy IS 'Hypercore proxy index access method';

CREATE OPERATOR CLASS int4_ops
DEFAULT FOR TYPE int4 USING hypercore_proxy AS
       OPERATOR 1 = (int4, int4),
       FUNCTION 1 hashint4(int4);

CREATE FUNCTION _timescaledb_debug.is_compressed_tid(tid) RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C STRICT;

DROP PROCEDURE IF EXISTS _timescaledb_functions.policy_compression_execute;
DROP FUNCTION IF EXISTS @extschema@.add_compression_policy;
DROP PROCEDURE IF EXISTS @extschema@.add_columnstore_policy;
DROP FUNCTION IF EXISTS timescaledb_experimental.add_policies;
DROP FUNCTION IF EXISTS @extschema@.compress_chunk;
DROP PROCEDURE IF EXISTS @extschema@.convert_to_columnstore;

CREATE FUNCTION @extschema@.compress_chunk(
  uncompressed_chunk REGCLASS,
  if_not_compressed BOOLEAN = true,
  recompress BOOLEAN = false,
  hypercore_use_access_method BOOL = NULL
) RETURNS REGCLASS AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C VOLATILE;

CREATE PROCEDURE @extschema@.convert_to_columnstore(
  chunk REGCLASS,
  if_not_columnstore BOOLEAN = true,
  recompress BOOLEAN = false,
  hypercore_use_access_method BOOL = NULL
) AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C;

CREATE FUNCTION @extschema@.add_compression_policy(
    hypertable REGCLASS,
    compress_after "any" = NULL,
    if_not_exists BOOL = false,
    schedule_interval INTERVAL = NULL,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL,
    compress_created_before INTERVAL = NULL,
    hypercore_use_access_method BOOL = NULL
)
RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C VOLATILE;

CREATE PROCEDURE @extschema@.add_columnstore_policy(
    hypertable REGCLASS,
    after "any" = NULL,
    if_not_exists BOOL = false,
    schedule_interval INTERVAL = NULL,
    initial_start TIMESTAMPTZ = NULL,
    timezone TEXT = NULL,
    created_before INTERVAL = NULL,
    hypercore_use_access_method BOOL = NULL
) LANGUAGE C AS '@MODULE_PATHNAME@', 'ts_update_placeholder';

CREATE OR REPLACE FUNCTION timescaledb_experimental.add_policies(
    relation REGCLASS,
    if_not_exists BOOL = false,
    refresh_start_offset "any" = NULL,
    refresh_end_offset "any" = NULL,
    compress_after "any" = NULL,
    drop_after "any" = NULL,
    hypercore_use_access_method BOOL = NULL)
RETURNS BOOL
AS '@MODULE_PATHNAME@', 'ts_update_placeholder'
LANGUAGE C VOLATILE;

CREATE PROCEDURE
_timescaledb_functions.policy_compression_execute(
  job_id              INTEGER,
  htid                INTEGER,
  lag                 ANYELEMENT,
  maxchunks           INTEGER,
  verbose_log         BOOLEAN,
  recompress_enabled  BOOLEAN,
  reindex_enabled     BOOLEAN,
  use_creation_time   BOOLEAN,
  useam               BOOLEAN = NULL)
AS $$ BEGIN END $$ LANGUAGE PLPGSQL;

DROP FUNCTION IF EXISTS _timescaledb_functions.generate_uuid_v7;
DROP FUNCTION IF EXISTS _timescaledb_functions.uuid_v7_from_timestamptz;
DROP FUNCTION IF EXISTS _timescaledb_functions.uuid_v7_from_timestamptz_zeroed;
DROP FUNCTION IF EXISTS _timescaledb_functions.timestamptz_from_uuid_v7;
DROP FUNCTION IF EXISTS _timescaledb_functions.uuid_version;

DELETE FROM _timescaledb_catalog.compression_algorithm WHERE id = 7 AND version = 1 AND name = 'COMPRESSION_ALGORITHM_UUID';

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

DROP TABLE _timescaledb_catalog.tempsettings;

CREATE INDEX compression_settings_compress_relid_idx ON _timescaledb_catalog.compression_settings (compress_relid);

GRANT SELECT ON _timescaledb_catalog.compression_settings TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.compression_settings', '');

DROP FUNCTION IF EXISTS _timescaledb_functions.jsonb_get_matching_index_entry(jsonb, text, text);

-- block downgrade if a table has NULL orderby setting (not allowed in 2.21)
DO $$
BEGIN
  IF EXISTS (
        SELECT 1
        FROM _timescaledb_catalog.compression_settings
        WHERE orderby IS NULL
        ) THEN
    RAISE EXCEPTION 'TimescaleDB 2.21 can not have NULL columnstore orderby settings. Use ALTER TABLE to configure them before downgrading.';
  END IF;
END
$$;

-- remove empty segmentby
UPDATE _timescaledb_catalog.compression_settings
SET segmentby = NULL
WHERE segmentby = '{}';
