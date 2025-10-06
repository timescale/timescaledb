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
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;

-- remove cagg trigger from all hypertables and chunks
DO $$
DECLARE
  rel regclass;
BEGIN
  FOR rel IN SELECT format('%I.%I', schema_name, table_name)::regclass
    FROM _timescaledb_catalog.hypertable ht
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS ts_cagg_invalidation_trigger ON %s;', rel);
  END LOOP;
  FOR rel IN SELECT format('%I.%I', schema_name, table_name)::regclass
    FROM _timescaledb_catalog.chunk ch
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS ts_cagg_invalidation_trigger ON %s;', rel);
  END LOOP;
END
$$;

DROP FUNCTION IF EXISTS _timescaledb_internal.continuous_agg_invalidation_trigger();
DROP FUNCTION IF EXISTS _timescaledb_functions.continuous_agg_invalidation_trigger();
DROP FUNCTION IF EXISTS _timescaledb_functions.has_invalidation_trigger(regclass);
