DO $$
BEGIN
    UPDATE _timescaledb_config.bgw_job
      SET config = config - 'max_successes_per_job' - 'max_failures_per_job',
          schedule_interval = '1 month'
    WHERE id = 3; -- system job retention

    RAISE WARNING 'job history configuration modified'
    USING DETAIL = 'The job history will keep full history for each job and run once each month.';
END
$$;

DROP VIEW IF EXISTS timescaledb_information.job_stats;
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;

-- add cagg trigger
DO $$
DECLARE
  hypertable regclass;
  hypertable_id integer;
BEGIN
  FOR hypertable_id, hypertable IN SELECT ht.id, format('%I.%I', schema_name, table_name)::regclass AS hypertable
  FROM _timescaledb_catalog.hypertable ht WHERE compression_state <> 2
    AND EXISTS (SELECT FROM _timescaledb_catalog.continuous_agg agg WHERE agg.raw_hypertable_id = ht.id);
  LOOP
    EXECUTE format('CREATE TRIGGER ts_cagg_invalidation_trigger AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION _timescaledb_functions.continuous_agg_invalidation_trigger(%s);', hypertable, hypertable_id);
  END LOOP;
END
$$;

