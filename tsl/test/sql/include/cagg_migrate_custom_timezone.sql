-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SELECT
    format('%2$s/results/cagg_migrate_%1$s.sql', lower(:'TIME_DIMENSION_DATATYPE'), :'TEST_OUTPUT_DIR') AS "TEST_SCHEMA_FILE"
\gset
SELECT timescaledb_pre_restore();
\ir :TEST_SCHEMA_FILE
SELECT timescaledb_post_restore();

-- Check restored watermark values
SELECT mat_hypertable_id, _timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(mat_hypertable_id))
   FROM _timescaledb_catalog.continuous_agg;

DROP MATERIALIZED VIEW conditions_summary_daily_new;
CALL cagg_migrate('conditions_summary_daily');

-- Cleanup
TRUNCATE _timescaledb_catalog.continuous_agg_migrate_plan RESTART IDENTITY CASCADE;
DROP MATERIALIZED VIEW conditions_summary_daily;
DROP MATERIALIZED VIEW conditions_summary_weekly;
DROP TABLE conditions CASCADE;
