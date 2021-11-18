
DROP PROCEDURE IF EXISTS recompress_chunk;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_status;

DO $$
DECLARE
    caggs text[];
    caggs_nr int;
BEGIN
    SELECT array_agg(format('%I.%I', user_view_schema, user_view_name)) FROM _timescaledb_catalog.continuous_agg WHERE bucket_width < 0 INTO caggs;
    SELECT array_length(caggs, 1) INTO caggs_nr;
    IF caggs_nr > 0 THEN
        RAISE EXCEPTION 'Downgrade is impossible since % continuous aggregates exist which use variable buckets: %', caggs_nr, caggs
              USING HINT = 'Remove the corresponding continuous aggregates manually before downgrading';
    END IF;

    -- It's safe to drop the table.
    -- ALTER EXTENSION is required to revert the effect of pg_extension_config_dump()
    -- See "The list of tables configured to be dumped" test in test/sql/updates/post.catalog.sql
    ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.continuous_aggs_bucket_function;

    -- Actually drop the table.
    -- ALTER EXTENSION only removes the table from the extension but doesn't drop it.
    DROP TABLE IF EXISTS _timescaledb_catalog.continuous_aggs_bucket_function;

    -- Drop overloaded versions of invalidation_process_hypertable_log() and invalidation_process_cagg_log()
    -- with bucket_functions argument.

    ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_internal.invalidation_process_hypertable_log(
        mat_hypertable_id INTEGER,
        raw_hypertable_id INTEGER,
        dimtype REGTYPE,
        mat_hypertable_ids INTEGER[],
        bucket_widths BIGINT[],
        max_bucket_widths BIGINT[],
        bucket_functions TEXT[]
    );

    DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_process_hypertable_log(
        mat_hypertable_id INTEGER,
        raw_hypertable_id INTEGER,
        dimtype REGTYPE,
        mat_hypertable_ids INTEGER[],
        bucket_widths BIGINT[],
        max_bucket_widths BIGINT[],
        bucket_functions TEXT[]
    );

    ALTER EXTENSION timescaledb DROP FUNCTION _timescaledb_internal.invalidation_process_cagg_log(
        mat_hypertable_id INTEGER,
        raw_hypertable_id INTEGER,
        dimtype REGTYPE,
        window_start BIGINT,
        window_end BIGINT,
        mat_hypertable_ids INTEGER[],
        bucket_widths BIGINT[],
        max_bucket_widths BIGINT[],
        bucket_functions TEXT[],
        OUT ret_window_start BIGINT,
        OUT ret_window_end BIGINT
    );

    DROP FUNCTION IF EXISTS _timescaledb_internal.invalidation_process_cagg_log(
        mat_hypertable_id INTEGER,
        raw_hypertable_id INTEGER,
        dimtype REGTYPE,
        window_start BIGINT,
        window_end BIGINT,
        mat_hypertable_ids INTEGER[],
        bucket_widths BIGINT[],
        max_bucket_widths BIGINT[],
        bucket_functions TEXT[],
        OUT ret_window_start BIGINT,
        OUT ret_window_end BIGINT
    );
END
$$ LANGUAGE 'plpgsql';

-- revert changes to continuous aggregates view definition
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;
