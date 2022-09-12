-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file contains functions and procedures to migrate old continuous
-- aggregate format to the finals form (without partials).

-- Check if exists a plan for migrationg a given cagg
CREATE OR REPLACE FUNCTION _timescaledb_internal.cagg_migrate_plan_exists (
    _hypertable_id INTEGER
)
RETURNS BOOLEAN
LANGUAGE sql AS
$BODY$
    SELECT EXISTS (
        SELECT 1
        FROM _timescaledb_catalog.continuous_agg_migrate_plan
        WHERE mat_hypertable_id = _hypertable_id
    );
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Execute all pre-validations required to execute the migration
CREATE OR REPLACE FUNCTION _timescaledb_internal.cagg_migrate_pre_validation (
    _cagg_schema TEXT,
    _cagg_name TEXT,
    _cagg_name_new TEXT
)
RETURNS _timescaledb_catalog.continuous_agg
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _cagg_data _timescaledb_catalog.continuous_agg;
BEGIN
    SELECT *
    INTO _cagg_data
    FROM _timescaledb_catalog.continuous_agg
    WHERE user_view_schema = _cagg_schema
    AND user_view_name = _cagg_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'continuous aggregate "%.%" does not exist', _cagg_schema, _cagg_name;
    END IF;

    IF _cagg_data.finalized IS TRUE THEN
        RAISE EXCEPTION 'continuous aggregate "%.%" does not require any migration', _cagg_schema, _cagg_name;
    END IF;

    IF _timescaledb_internal.cagg_migrate_plan_exists(_cagg_data.mat_hypertable_id) IS TRUE THEN
        RAISE EXCEPTION 'plan already exists for continuous aggregate %.%', _cagg_schema, _cagg_name;
    END IF;

    IF EXISTS (
        SELECT finalized
        FROM _timescaledb_catalog.continuous_agg
        WHERE user_view_schema = _cagg_schema
        AND user_view_name = _cagg_name_new
    ) THEN
        RAISE EXCEPTION 'continuous aggregate "%.%" already exists', _cagg_schema, _cagg_name_new;
    END IF;

    RETURN _cagg_data;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Create migration plan for given cagg
CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_create_plan (
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

-- Create new cagg using the new format
CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_create_new_cagg (
    _cagg_data _timescaledb_catalog.continuous_agg,
    _plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _view_def TEXT;
BEGIN
    _view_def := left(pg_get_viewdef(format('%I.%I', _cagg_data.direct_view_schema, _cagg_data.direct_view_name), false), -1);
    _view_def := format(
        'CREATE MATERIALIZED VIEW %I.%I WITH (timescaledb.continuous, timescaledb.materialized_only=%L) AS %s WITH NO DATA;',
        _cagg_data.user_view_schema,
        _plan_step.config->>'cagg_name_new',
        _cagg_data.materialized_only,
        _view_def);

    EXECUTE _view_def;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Disable policies
CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_disable_policies (
    _cagg_data _timescaledb_catalog.continuous_agg,
    _plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _policies INTEGER[];
BEGIN
    IF _plan_step.config->>'policies' IS NOT NULL THEN
        SELECT array_agg(value::integer)
        INTO _policies
        FROM jsonb_array_elements_text( (_plan_step.config->'policies') );

        UPDATE _timescaledb_config.bgw_job
        SET scheduled = FALSE
        WHERE id = ANY(_policies)
        AND scheduled IS TRUE;
    END IF;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Enable policies
CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_enable_policies (
    _cagg_data _timescaledb_catalog.continuous_agg,
    _plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _policies INTEGER[];
BEGIN
    IF _plan_step.config->>'policies' IS NOT NULL THEN
        SELECT array_agg(value::integer)
        INTO _policies
        FROM jsonb_array_elements_text( (_plan_step.config->'policies') );

        UPDATE _timescaledb_config.bgw_job
        SET scheduled = TRUE
        WHERE id = ANY(_policies)
        AND scheduled IS FALSE;
    END IF;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Copy policies
CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_copy_policies (
    _cagg_data _timescaledb_catalog.continuous_agg,
    _plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _mat_hypertable_id INTEGER;
    _policies INTEGER[];
    _new_policies INTEGER[];
BEGIN
    IF _plan_step.config->>'policies' IS NULL THEN
        RETURN;
    END IF;

    SELECT array_agg(value::integer)
    INTO _policies
    FROM jsonb_array_elements_text( (_plan_step.config->'policies') );

    SELECT h.id
    INTO _mat_hypertable_id
    FROM _timescaledb_catalog.continuous_agg ca
    JOIN _timescaledb_catalog.hypertable h ON (h.id = ca.mat_hypertable_id)
    WHERE user_view_schema = _cagg_data.user_view_schema
    AND user_view_name = _plan_step.config->>'cagg_name_new';

    WITH jobs AS (
        SELECT application_name, schedule_interval, max_runtime, max_retries, retry_period, proc_schema, proc_name, owner, scheduled, config
        FROM _timescaledb_config.bgw_job
        WHERE id = ANY(_policies)
    ),
    insert_new_policies AS (
        INSERT INTO
            _timescaledb_config.bgw_job (
                application_name, schedule_interval, max_runtime, max_retries,
                retry_period, proc_schema, proc_name, owner, scheduled, hypertable_id, config
            )
        SELECT application_name, schedule_interval, max_runtime, max_retries, retry_period,
               proc_schema, proc_name, owner, scheduled, _mat_hypertable_id, config
        FROM jobs
        RETURNING id
    )
    SELECT array_agg(id)
    INTO _new_policies
    FROM insert_new_policies;

    UPDATE _timescaledb_catalog.continuous_agg_migrate_plan_step
    SET config = json_build_object('policies', _new_policies)
    WHERE type = 'ENABLE POLICIES'
    AND mat_hypertable_id = _plan_step.mat_hypertable_id;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Refresh new cagg created by the migration
CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_refresh_new_cagg (
    _cagg_data _timescaledb_catalog.continuous_agg,
    _plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _cagg REGCLASS;
BEGIN
    _cagg := format('%I.%I', _cagg_data.user_view_schema, _plan_step.config->>'cagg_name_new')::REGCLASS;

    CASE _plan_step.config->>'window_start_type'
        WHEN 'timestamp with time zone' THEN
            CALL @extschema@.refresh_continuous_aggregate(_cagg, (_plan_step.config->>'window_start')::timestamptz, NULL);
        WHEN 'bigint' THEN
            CALL @extschema@.refresh_continuous_aggregate(_cagg, (_plan_step.config->>'window_start')::bigint, NULL);
        WHEN 'integer' THEN
            CALL @extschema@.refresh_continuous_aggregate(_cagg, (_plan_step.config->>'window_start')::integer, NULL);
        WHEN 'smallint' THEN
            CALL @extschema@.refresh_continuous_aggregate(_cagg, (_plan_step.config->>'window_start')::smallint, NULL);
        ELSE
            RAISE EXCEPTION 'Invalid data type % for time bucket', _plan_step.config->>'window_start_type';
    END CASE;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Copy data from the OLD cagg to the new Materialization Hypertable
CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_copy_data (
    _cagg_data _timescaledb_catalog.continuous_agg,
    _plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _stmt TEXT;
    _mat_schema_name TEXT;
    _mat_table_name TEXT;
BEGIN
    SELECT h.schema_name, h.table_name
    INTO _mat_schema_name, _mat_table_name
    FROM _timescaledb_catalog.continuous_agg ca
    JOIN _timescaledb_catalog.hypertable h ON (h.id = ca.mat_hypertable_id)
    WHERE user_view_schema = _cagg_data.user_view_schema
    AND user_view_name = _plan_step.config->>'cagg_name_new';

    _stmt := format(
        'INSERT INTO %I.%I SELECT * FROM %I.%I WHERE %I >= %L AND %I < %L',
        _mat_schema_name,
        _mat_table_name,
        _cagg_data.user_view_schema,
        _cagg_data.user_view_name,
        _plan_step.config->>'bucket_column_name',
        _plan_step.config->>'start_ts',
        _plan_step.config->>'bucket_column_name',
        _plan_step.config->>'end_ts'
    );

    EXECUTE _stmt;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Rename the new cagg using `_old` suffix and rename the `_new` to the original name
CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_override_cagg (
    _cagg_data _timescaledb_catalog.continuous_agg,
    _plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _stmt TEXT;
BEGIN
    IF (_plan_step.config->>'override')::BOOLEAN IS FALSE THEN
        RETURN;
    END IF;

    _stmt := 'ALTER MATERIALIZED VIEW %I.%I RENAME TO %I;';

    IF (_plan_step.config->>'override')::BOOLEAN IS TRUE THEN
        EXECUTE format (
            _stmt,
            _cagg_data.user_view_schema, _cagg_data.user_view_name,
            replace(_plan_step.config->>'cagg_name_new', '_new', '_old')
        );

        EXECUTE format (
            _stmt,
            _cagg_data.user_view_schema, _plan_step.config->>'cagg_name_new',
            _cagg_data.user_view_name
        );
    END IF;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Remove old cagg if the parameter `drop_old` and `override` is true
CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_drop_old_cagg (
    _cagg_data _timescaledb_catalog.continuous_agg,
    _plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _stmt TEXT;
BEGIN
    IF (_plan_step.config->>'drop_old')::BOOLEAN IS FALSE THEN
        RETURN;
    END IF;

    _stmt := 'DROP MATERIALIZED VIEW %I.%I;';

    IF (_plan_step.config->>'override')::BOOLEAN IS TRUE THEN
        EXECUTE format (
            _stmt,
            _cagg_data.user_view_schema, replace(_plan_step.config->>'cagg_name_new', '_new', '_old')
        );
    END IF;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Execute the migration plan, step by step
CREATE OR REPLACE PROCEDURE _timescaledb_internal.cagg_migrate_execute_plan (
    _cagg_data _timescaledb_catalog.continuous_agg
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step;
    _call_stmt TEXT;
BEGIN
    FOR _plan_step IN
        SELECT *
        FROM _timescaledb_catalog.continuous_agg_migrate_plan_step
        WHERE mat_hypertable_id OPERATOR(pg_catalog.=) _cagg_data.mat_hypertable_id
        AND status OPERATOR(pg_catalog.=) ANY (ARRAY['NOT STARTED', 'STARTED'])
        ORDER BY step_id
    LOOP
        -- change the status of the step
        UPDATE _timescaledb_catalog.continuous_agg_migrate_plan_step
        SET status = 'STARTED', start_ts = pg_catalog.clock_timestamp()
        WHERE mat_hypertable_id OPERATOR(pg_catalog.=) _plan_step.mat_hypertable_id
        AND step_id OPERATOR(pg_catalog.=) _plan_step.step_id;
        COMMIT;

        -- reload the step data for enable policies because the COPY DATA step update it
        IF _plan_step.type OPERATOR(pg_catalog.=) 'ENABLE POLICIES' THEN
            SELECT *
            INTO _plan_step
            FROM _timescaledb_catalog.continuous_agg_migrate_plan_step
            WHERE mat_hypertable_id OPERATOR(pg_catalog.=) _plan_step.mat_hypertable_id
            AND step_id OPERATOR(pg_catalog.=) _plan_step.step_id;
        END IF;

        -- execute step migration
        _call_stmt := pg_catalog.format('CALL _timescaledb_internal.cagg_migrate_execute_%s($1, $2)', pg_catalog.lower(pg_catalog.replace(_plan_step.type, ' ', '_')));
        EXECUTE _call_stmt USING _cagg_data, _plan_step;

        UPDATE _timescaledb_catalog.continuous_agg_migrate_plan_step
        SET status = 'FINISHED', end_ts = pg_catalog.clock_timestamp()
        WHERE mat_hypertable_id OPERATOR(pg_catalog.=) _plan_step.mat_hypertable_id
        AND step_id OPERATOR(pg_catalog.=) _plan_step.step_id;
        COMMIT;
    END LOOP;
END;
$BODY$;

-- Execute the entire migration
CREATE OR REPLACE PROCEDURE @extschema@.cagg_migrate (
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
