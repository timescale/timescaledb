
-- gapfill with timezone support
CREATE FUNCTION @extschema@.time_bucket_gapfill(bucket_width INTERVAL, ts TIMESTAMPTZ, timezone TEXT, start TIMESTAMPTZ=NULL, finish TIMESTAMPTZ=NULL) RETURNS TIMESTAMPTZ
AS '@MODULE_PATHNAME@', 'ts_gapfill_timestamptz_timezone_bucket' LANGUAGE C VOLATILE PARALLEL SAFE;

ALTER TABLE _timescaledb_catalog.compression_chunk_size DROP CONSTRAINT compression_chunk_size_pkey;
ALTER TABLE _timescaledb_catalog.compression_chunk_size ADD CONSTRAINT compression_chunk_size_pkey PRIMARY KEY(chunk_id);

DROP PROCEDURE IF EXISTS @extschema@.cagg_migrate (REGCLASS, BOOLEAN, BOOLEAN);

CREATE PROCEDURE @extschema@.cagg_migrate (
    cagg REGCLASS,
    override BOOLEAN DEFAULT FALSE,
    drop_old BOOLEAN DEFAULT FALSE
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _cagg_schema TEXT;
    _cagg_name TEXT;
    _cagg_name_new TEXT;
    _cagg_data _timescaledb_catalog.continuous_agg;
BEGIN
    SELECT nspname, relname
    INTO _cagg_schema, _cagg_name
    FROM pg_catalog.pg_class
    JOIN pg_catalog.pg_namespace ON pg_namespace.oid OPERATOR(pg_catalog.=) pg_class.relnamespace
    WHERE pg_class.oid OPERATOR(pg_catalog.=) cagg::pg_catalog.oid;

    -- maximum size of an identifier in Postgres is 63 characters, se we need to left space for '_new'
    _cagg_name_new := pg_catalog.format('%s_new', pg_catalog.substr(_cagg_name, 1, 59));

    -- pre-validate the migration and get some variables
    _cagg_data := _timescaledb_internal.cagg_migrate_pre_validation(_cagg_schema, _cagg_name, _cagg_name_new);

    -- create new migration plan
    CALL _timescaledb_internal.cagg_migrate_create_plan(_cagg_data, _cagg_name_new, override, drop_old);
    COMMIT;

    -- execute the migration plan
    CALL _timescaledb_internal.cagg_migrate_execute_plan(_cagg_data);

    -- finish the migration plan
    UPDATE _timescaledb_catalog.continuous_agg_migrate_plan
    SET end_ts = pg_catalog.clock_timestamp()
    WHERE mat_hypertable_id OPERATOR(pg_catalog.=) _cagg_data.mat_hypertable_id;
END;
$BODY$;
