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

DO
$$
DECLARE
    cagg_names TEXT;
BEGIN
    WITH cagg AS (
        SELECT
            format('%I.%I', user_view_schema, user_view_name) AS cagg_name
        FROM
            _timescaledb_catalog.continuous_agg
            JOIN _timescaledb_config.bgw_job ON bgw_job.hypertable_id = continuous_agg.mat_hypertable_id
        WHERE
            parent_mat_hypertable_id IS NOT NULL
        GROUP BY
            1
        HAVING
            count(*) > 1
    )
    SELECT
        string_agg(cagg_name, ', ' ORDER BY cagg_name)
    INTO
        cagg_names
    FROM
        cagg;

    IF cagg_names IS NOT NULL THEN
        RAISE WARNING 'hierarchical continuous aggregates with multiple refresh jobs found'
        USING
            DETAIL = 'The following continuous aggregates have multiple refresh jobs: '|| cagg_names,
            HINT = 'Consider consolidating the refresh jobs for these continuous aggregates due to potential deadlocks.';
    END IF;
END;
$$;
