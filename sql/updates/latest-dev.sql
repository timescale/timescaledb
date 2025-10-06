DO $$
BEGIN
    UPDATE _timescaledb_config.bgw_job
      SET config = config || '{"max_successes_per_job": 1000, "max_failures_per_job": 1000}',
          schedule_interval = '6 hours'
    WHERE id = 3; -- system job retention

    RAISE WARNING 'job history configuration modified'
    USING DETAIL = 'The job history will only keep the last 1000 successes and failures and run once each day.';
END
$$;

DROP VIEW IF EXISTS timescaledb_information.job_stats;


-- Fix wrong migration by removing all sparse index configurations
-- which only contain auto sparse indexing definitions on hypertable
DO $$
DECLARE
    rec RECORD;
    num_config INT;
BEGIN
	IF NOT EXISTS (
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = '_timescaledb_catalog'
		AND table_name = 'compression_settings'
		AND column_name = 'index') THEN
		RETURN;
	END IF;

    FOR rec IN
        SELECT relid, compress_relid, index
        FROM _timescaledb_catalog.compression_settings
		WHERE compress_relid IS NULL
    LOOP
		num_config:=0;

		SELECT count(*)
		INTO num_config
		FROM jsonb_array_elements(rec.index) AS idx
		WHERE idx::jsonb @> '{"storage":"config"}'::jsonb;

		IF num_config = 0 THEN
            UPDATE _timescaledb_catalog.compression_settings
            SET index = NULL
            WHERE relid = rec.relid;
		END IF;
    END LOOP;
END $$;

CREATE OR REPLACE FUNCTION _timescaledb_functions.cagg_migrate_pre_validation (
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

    IF _timescaledb_functions.cagg_migrate_plan_exists(_cagg_data.mat_hypertable_id) IS TRUE THEN
        RAISE EXCEPTION 'plan already exists for continuous aggregate %.%', _cagg_schema, _cagg_name;
    END IF;

    IF EXISTS (
        SELECT
        FROM _timescaledb_catalog.continuous_agg
        WHERE user_view_schema = _cagg_schema
        AND user_view_name = _cagg_name_new
        AND NOT EXISTS (
            SELECT FROM _timescaledb_catalog.continuous_agg_migrate_plan
            WHERE mat_hypertable_id = _cagg_data.mat_hypertable_id
            AND end_ts IS NULL
        )
    ) THEN
        RAISE EXCEPTION 'continuous aggregate "%.%" already exists', _cagg_schema, _cagg_name_new;
    END IF;

    RETURN _cagg_data;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE PROCEDURE _timescaledb_functions.cagg_migrate_execute_copy_data (
    _cagg_data _timescaledb_catalog.continuous_agg,
    _plan_step _timescaledb_catalog.continuous_agg_migrate_plan_step
)
LANGUAGE plpgsql AS
$BODY$
DECLARE
    _stmt TEXT;
    _mat_schema_name TEXT;
    _mat_table_name TEXT;
    _mat_schema_name_old TEXT;
    _mat_table_name_old TEXT;
    _query TEXT;
    _select_columns TEXT;
    _groupby_columns TEXT;
BEGIN
    SELECT h.schema_name, h.table_name
    INTO _mat_schema_name, _mat_table_name
    FROM _timescaledb_catalog.continuous_agg ca
    JOIN _timescaledb_catalog.hypertable h ON (h.id = ca.mat_hypertable_id)
    WHERE user_view_schema = _cagg_data.user_view_schema
    AND user_view_name = _plan_step.config->>'cagg_name_new';

    -- For realtime CAggs we need to read direct from the materialization hypertable
    IF _cagg_data.materialized_only IS FALSE THEN
        SELECT h.schema_name, h.table_name
        INTO _mat_schema_name_old, _mat_table_name_old
        FROM _timescaledb_catalog.continuous_agg ca
        JOIN _timescaledb_catalog.hypertable h ON (h.id = ca.mat_hypertable_id)
        WHERE user_view_schema = _cagg_data.user_view_schema
        AND user_view_name = _cagg_data.user_view_name;

        _query :=
            split_part(
                pg_get_viewdef(format('%I.%I', _cagg_data.user_view_schema, _cagg_data.user_view_name)),
                'UNION ALL',
                1);

        _groupby_columns :=
            split_part(
                _query,
                'GROUP BY ',
                2);

        _select_columns :=
            split_part(
                _query,
                format('FROM %I.%I', _mat_schema_name_old, _mat_table_name_old),
                1);

        _stmt := format(
            'INSERT INTO %I.%I %s FROM %I.%I WHERE %I >= %L AND %I < %L GROUP BY %s',
            _mat_schema_name,
            _mat_table_name,
            _select_columns,
            _mat_schema_name_old,
            _mat_table_name_old,
            _plan_step.config->>'bucket_column_name',
            _plan_step.config->>'start_ts',
            _plan_step.config->>'bucket_column_name',
            _plan_step.config->>'end_ts',
            _groupby_columns
        );
    ELSE
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
    END IF;

    EXECUTE _stmt;
END;
$BODY$ SET search_path TO pg_catalog, pg_temp;
