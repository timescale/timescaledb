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

CREATE OR REPLACE FUNCTION _timescaledb_functions.continuous_agg_invalidation_trigger() RETURNS TRIGGER AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C;

-- add cagg trigger to hypertables with caggs and their chunks
DO $$
DECLARE
  v_hypertable regclass;
  v_chunk regclass;
  v_hypertable_id integer;
BEGIN
  FOR v_hypertable_id, v_hypertable IN SELECT ht.id, format('%I.%I', schema_name, table_name)::regclass
  FROM _timescaledb_catalog.hypertable ht WHERE compression_state <> 2
    AND EXISTS (SELECT FROM _timescaledb_catalog.continuous_agg agg WHERE agg.raw_hypertable_id = ht.id)
  LOOP
    EXECUTE format('CREATE TRIGGER ts_cagg_invalidation_trigger AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION _timescaledb_functions.continuous_agg_invalidation_trigger(''%s'');', v_hypertable, v_hypertable_id);
    FOR v_chunk IN SELECT format('%I.%I', schema_name, table_name)::regclass
      FROM _timescaledb_catalog.chunk ch WHERE ch.hypertable_id = v_hypertable_id
    LOOP
      EXECUTE format('CREATE TRIGGER ts_cagg_invalidation_trigger AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION _timescaledb_functions.continuous_agg_invalidation_trigger(''%s'');', v_chunk, v_hypertable_id);
    END LOOP;
  END LOOP;
END
$$;

CREATE OR REPLACE FUNCTION _timescaledb_functions.insert_blocker() RETURNS TRIGGER AS '@MODULE_PATHNAME@', 'ts_update_placeholder' LANGUAGE C;

-- add ts_insert_blocker trigger to hypertables
DO $$
DECLARE
  v_hypertable regclass;
BEGIN
  FOR v_hypertable IN SELECT format('%I.%I', schema_name, table_name)::regclass
  FROM _timescaledb_catalog.hypertable ht
  LOOP
    EXECUTE format('CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON %s FOR EACH ROW EXECUTE FUNCTION _timescaledb_functions.insert_blocker();', v_hypertable);
  END LOOP;
END
$$;

