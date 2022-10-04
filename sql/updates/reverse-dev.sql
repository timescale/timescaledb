
-- gapfill with timezone support
DROP FUNCTION @extschema@.time_bucket_gapfill(INTERVAL,TIMESTAMPTZ,TEXT,TIMESTAMPTZ,TIMESTAMPTZ);

ALTER TABLE _timescaledb_catalog.compression_chunk_size DROP CONSTRAINT compression_chunk_size_pkey;
ALTER TABLE _timescaledb_catalog.compression_chunk_size ADD CONSTRAINT compression_chunk_size_pkey PRIMARY KEY(chunk_id,compressed_chunk_id);

DROP PROCEDURE @extschema@.cagg_migrate (REGCLASS, BOOLEAN, BOOLEAN);
DROP PROCEDURE _timescaledb_internal.cagg_migrate_create_plan (_timescaledb_catalog.continuous_agg, TEXT, BOOLEAN, BOOLEAN);

CREATE PROCEDURE _timescaledb_internal.cagg_migrate_create_plan (
    _cagg_data _timescaledb_catalog.continuous_agg,
    _cagg_name_new TEXT,
    _override BOOLEAN DEFAULT FALSE,
    _drop_old BOOLEAN DEFAULT FALSE
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _sql TEXT;
    _matht RECORD;
    _time_interval INTERVAL;
    _integer_interval BIGINT;
    _watermark TEXT;
    _policies JSONB;
    _bucket_column_name TEXT;
    _bucket_column_type TEXT;
    _interval_type TEXT;
    _interval_value TEXT;
BEGIN
    IF _timescaledb_internal.cagg_migrate_plan_exists(_cagg_data.mat_hypertable_id) IS TRUE THEN
        RAISE EXCEPTION 'plan already exists for materialized hypertable %', _cagg_data.mat_hypertable_id;
    END IF;

    INSERT INTO
        _timescaledb_catalog.continuous_agg_migrate_plan (mat_hypertable_id)
    VALUES
        (_cagg_data.mat_hypertable_id);

    SELECT schema_name, table_name
    INTO _matht
    FROM _timescaledb_catalog.hypertable
    WHERE id = _cagg_data.mat_hypertable_id;

    SELECT time_interval, integer_interval, column_name, column_type
    INTO _time_interval, _integer_interval, _bucket_column_name, _bucket_column_type
    FROM timescaledb_information.dimensions
    WHERE hypertable_schema = _matht.schema_name
    AND hypertable_name = _matht.table_name
    AND dimension_type = 'Time';

    IF _integer_interval IS NOT NULL THEN
        _interval_value := _integer_interval::TEXT;
        _interval_type  := _bucket_column_type;
        IF _bucket_column_type = 'bigint' THEN
            _watermark := COALESCE(_timescaledb_internal.cagg_watermark(_cagg_data.mat_hypertable_id)::bigint, '-9223372036854775808'::bigint)::TEXT;
        ELSIF _bucket_column_type = 'integer' THEN
            _watermark := COALESCE(_timescaledb_internal.cagg_watermark(_cagg_data.mat_hypertable_id)::integer, '-2147483648'::integer)::TEXT;
        ELSE
            _watermark := COALESCE(_timescaledb_internal.cagg_watermark(_cagg_data.mat_hypertable_id)::smallint, '-32768'::smallint)::TEXT;
        END IF;
    ELSE
        _interval_value := _time_interval::TEXT;
        _interval_type  := 'interval';
        _watermark      := COALESCE(_timescaledb_internal.to_timestamp(_timescaledb_internal.cagg_watermark(_cagg_data.mat_hypertable_id)), '-infinity'::timestamptz)::TEXT;
    END IF;

    -- get all scheduled policies except the refresh
    SELECT jsonb_build_object('policies', array_agg(id))
    INTO _policies
    FROM _timescaledb_config.bgw_job
    WHERE hypertable_id = _cagg_data.mat_hypertable_id
    AND proc_name IS DISTINCT FROM 'policy_refresh_continuous_aggregate'
    AND scheduled IS TRUE
    AND id >= 1000;

    INSERT INTO
        _timescaledb_catalog.continuous_agg_migrate_plan_step (mat_hypertable_id, type, config)
    VALUES
        (_cagg_data.mat_hypertable_id, 'SAVE WATERMARK', jsonb_build_object('watermark', _watermark)),
        (_cagg_data.mat_hypertable_id, 'CREATE NEW CAGG', jsonb_build_object('cagg_name_new', _cagg_name_new)),
        (_cagg_data.mat_hypertable_id, 'DISABLE POLICIES', _policies),
        (_cagg_data.mat_hypertable_id, 'REFRESH NEW CAGG', jsonb_build_object('cagg_name_new', _cagg_name_new, 'window_start', _watermark, 'window_start_type', _bucket_column_type));

    -- Finish the step because don't require any extra step
    UPDATE _timescaledb_catalog.continuous_agg_migrate_plan_step
    SET status = 'FINISHED', start_ts = now(), end_ts = clock_timestamp()
    WHERE type = 'SAVE WATERMARK';

    _sql := format (
        $$
        WITH boundaries AS (
            SELECT min(%1$I), max(%1$I), %1$L AS bucket_column_name, %2$L AS bucket_column_type, %3$L AS cagg_name_new
            FROM %4$I.%5$I
            WHERE %1$I < CAST(%6$L AS %2$s)
        )
        INSERT INTO
            _timescaledb_catalog.continuous_agg_migrate_plan_step (mat_hypertable_id, type, config)
        SELECT
            %7$L,
            'COPY DATA',
            jsonb_build_object (
                'start_ts', start::text,
                'end_ts', (start + CAST(%8$L AS %9$s))::text,
                'bucket_column_name', bucket_column_name,
                'bucket_column_type', bucket_column_type,
                'cagg_name_new', cagg_name_new
            )
        FROM boundaries,
             LATERAL generate_series(min, max, CAST(%8$L AS %9$s)) AS start;
        $$,
        _bucket_column_name, _bucket_column_type, _cagg_name_new, _cagg_data.user_view_schema,
        _cagg_data.user_view_name, _watermark, _cagg_data.mat_hypertable_id, _interval_value, _interval_type
    );

    EXECUTE _sql;

    INSERT INTO
        _timescaledb_catalog.continuous_agg_migrate_plan_step (mat_hypertable_id, type, config)
    VALUES
        (_cagg_data.mat_hypertable_id, 'OVERRIDE CAGG', jsonb_build_object('cagg_name_new', _cagg_name_new, 'override', _override, 'drop_old', _drop_old)),
        (_cagg_data.mat_hypertable_id, 'DROP OLD CAGG', jsonb_build_object('cagg_name_new', _cagg_name_new, 'override', _override, 'drop_old', _drop_old)),
        (_cagg_data.mat_hypertable_id, 'COPY POLICIES', _policies || jsonb_build_object('cagg_name_new', _cagg_name_new)),
        (_cagg_data.mat_hypertable_id, 'ENABLE POLICIES', NULL);
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE PROCEDURE @extschema@.cagg_migrate (
    _cagg REGCLASS,
    _override BOOLEAN DEFAULT FALSE,
    _drop_old BOOLEAN DEFAULT FALSE
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
    WHERE pg_class.oid OPERATOR(pg_catalog.=) _cagg::pg_catalog.oid;

    -- maximum size of an identifier in Postgres is 63 characters, se we need to left space for '_new'
    _cagg_name_new := pg_catalog.format('%s_new', pg_catalog.substr(_cagg_name, 1, 59));

    -- pre-validate the migration and get some variables
    _cagg_data := _timescaledb_internal.cagg_migrate_pre_validation(_cagg_schema, _cagg_name, _cagg_name_new);

    -- create new migration plan
    CALL _timescaledb_internal.cagg_migrate_create_plan(_cagg_data, _cagg_name_new, _override, _drop_old);
    COMMIT;

    -- execute the migration plan
    CALL _timescaledb_internal.cagg_migrate_execute_plan(_cagg_data);

    -- finish the migration plan
    UPDATE _timescaledb_catalog.continuous_agg_migrate_plan
    SET end_ts = pg_catalog.clock_timestamp()
    WHERE mat_hypertable_id OPERATOR(pg_catalog.=) _cagg_data.mat_hypertable_id;
END;
$BODY$;

-- Issue #4727
ALTER TABLE _timescaledb_catalog.continuous_agg_migrate_plan_step
    DROP CONSTRAINT IF EXISTS continuous_agg_migrate_plan_step_check2;

ALTER TABLE _timescaledb_catalog.continuous_agg_migrate_plan_step
    ADD CONSTRAINT continuous_agg_migrate_plan_step_check2
    CHECK (type IN ('CREATE NEW CAGG', 'DISABLE POLICIES', 'COPY POLICIES', 'ENABLE POLICIES', 'SAVE WATERMARK', 'REFRESH NEW CAGG', 'COPY DATA'));

DROP FUNCTION _timescaledb_internal.policy_job_error_retention(integer, JSONB);
DROP FUNCTION _timescaledb_internal.policy_job_error_retention_check(JSONB);
DELETE FROM _timescaledb_config.bgw_job WHERE id = 2;

ALTER EXTENSION timescaledb DROP VIEW timescaledb_information.job_errors;
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_internal.job_errors;

DROP VIEW timescaledb_information.job_errors;
DROP TABLE _timescaledb_internal.job_errors;

-- drop dependent views
DROP VIEW IF EXISTS timescaledb_information.job_stats;
DROP VIEW IF EXISTS timescaledb_information.jobs;

ALTER TABLE _timescaledb_internal.bgw_job_stat
DROP COLUMN flags;
-- need to recreate the bgw_job_stats table because dropping the column
-- will not remove it from the pg_attribute table

CREATE TABLE _timescaledb_internal.bgw_job_stat_tmp (
    LIKE _timescaledb_internal.bgw_job_stat
    INCLUDING ALL
    -- indexes and constraintes will be created later to keep original names
    EXCLUDING INDEXES
    EXCLUDING CONSTRAINTS
);

INSERT INTO _timescaledb_internal.bgw_job_stat_tmp
SELECT
    job_id,
    last_start,
    last_finish,
    next_start,
    last_successful_finish,
    last_run_success,
    total_runs,
    total_duration,
    total_successes,
    total_failures,
    total_crashes,
    consecutive_failures,
    consecutive_crashes
FROM
    _timescaledb_internal.bgw_job_stat;

DROP TABLE _timescaledb_internal.bgw_job_stat;

ALTER TABLE _timescaledb_internal.bgw_job_stat_tmp
    RENAME TO bgw_job_stat;
ALTER TABLE _timescaledb_internal.bgw_job_stat
    ADD CONSTRAINT bgw_job_stat_pkey PRIMARY KEY (job_id),
    ADD CONSTRAINT bgw_job_stat_job_id_fkey FOREIGN KEY (job_id)
    REFERENCES _timescaledb_config.bgw_job (id) ON DELETE CASCADE;

GRANT SELECT ON TABLE _timescaledb_internal.bgw_job_stat TO PUBLIC;

DROP VIEW _timescaledb_internal.hypertable_chunk_local_size;
DROP FUNCTION _timescaledb_internal.hypertable_local_size(name, name);

CREATE FUNCTION _timescaledb_internal.hypertable_local_size(
	schema_name_in name,
	table_name_in name)
RETURNS TABLE (
	table_bytes BIGINT,
	index_bytes BIGINT,
	toast_bytes BIGINT,
	total_bytes BIGINT)
LANGUAGE SQL VOLATILE STRICT AS
$BODY$
    /* get the main hypertable id and sizes */
    WITH _hypertable AS (
        SELECT
            id,
            _timescaledb_internal.relation_size(format('%I.%I', schema_name, table_name)::regclass) AS relsize
        FROM
            _timescaledb_catalog.hypertable
        WHERE
            schema_name = schema_name_in
            AND table_name = table_name_in
    ),
    /* project the size of the parent hypertable */
    _hypertable_sizes AS (
        SELECT
            id,
            COALESCE((relsize).total_size, 0) AS total_bytes,
            COALESCE((relsize).heap_size, 0) AS heap_bytes,
            COALESCE((relsize).index_size, 0) AS index_bytes,
            COALESCE((relsize).toast_size, 0) AS toast_bytes,
            0::BIGINT AS compressed_total_size,
            0::BIGINT AS compressed_index_size,
            0::BIGINT AS compressed_toast_size,
            0::BIGINT AS compressed_heap_size
        FROM
            _hypertable
    ),
    /* calculate the size of the hypertable chunks */
    _chunk_sizes AS (
        SELECT
            chunk_id,
            COALESCE(ch.total_bytes, 0) AS total_bytes,
            COALESCE(ch.heap_bytes, 0) AS heap_bytes,
            COALESCE(ch.index_bytes, 0) AS index_bytes,
            COALESCE(ch.toast_bytes, 0) AS toast_bytes,
            COALESCE(ch.compressed_total_size, 0) AS compressed_total_size,
            COALESCE(ch.compressed_index_size, 0) AS compressed_index_size,
            COALESCE(ch.compressed_toast_size, 0) AS compressed_toast_size,
            COALESCE(ch.compressed_heap_size, 0) AS compressed_heap_size
        FROM
            _timescaledb_internal.hypertable_chunk_local_size ch
            JOIN _hypertable_sizes ht ON ht.id = ch.hypertable_id
    )
    /* calculate the SUM of the hypertable and chunk sizes */
	SELECT
		(SUM(heap_bytes)  + SUM(compressed_heap_size))::BIGINT AS heap_bytes,
		(SUM(index_bytes) + SUM(compressed_index_size))::BIGINT AS index_bytes,
		(SUM(toast_bytes) + SUM(compressed_toast_size))::BIGINT AS toast_bytes,
		(SUM(total_bytes) + SUM(compressed_total_size))::BIGINT AS total_bytes
	FROM
		(SELECT * FROM _hypertable_sizes
         UNION ALL
         SELECT * FROM _chunk_sizes) AS sizes;
$BODY$ SET search_path TO pg_catalog, pg_temp;

