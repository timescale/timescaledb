
DROP FUNCTION IF EXISTS detach_data_node(name,regclass,boolean,boolean);
DROP FUNCTION IF EXISTS distributed_exec;

DROP PROCEDURE IF EXISTS refresh_continuous_aggregate(regclass,"any","any");

DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;

-- Rebuild hypertable invalidation log
CREATE TABLE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log_tmp AS
SELECT hypertable_id, lowest_modified_value, greatest_modified_value
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

ALTER EXTENSION timescaledb
      DROP TABLE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;
DROP TABLE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log;

CREATE TABLE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log (
  hypertable_id integer NOT NULL,
  lowest_modified_value bigint NOT NULL,
  greatest_modified_value bigint NOT NULL
);

INSERT INTO _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
SELECT * FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log_tmp;

DROP TABLE _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log_tmp;

SELECT pg_catalog.pg_extension_config_dump(
       '_timescaledb_catalog.continuous_aggs_hypertable_invalidation_log', '');

CREATE INDEX continuous_aggs_hypertable_invalidation_log_idx ON
    _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log (
        hypertable_id, lowest_modified_value ASC);
GRANT SELECT ON  _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log TO PUBLIC;

-- Rebuild materialization invalidation log
CREATE TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log_tmp AS
SELECT materialization_id, lowest_modified_value, greatest_modified_value
FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;

ALTER EXTENSION timescaledb
      DROP TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;
DROP TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log;

CREATE TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log (
  materialization_id integer
      REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id) ON DELETE CASCADE,
  lowest_modified_value bigint NOT NULL,
  greatest_modified_value bigint NOT NULL
);

INSERT INTO _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
SELECT * FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log_tmp;

DROP TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log_tmp;

SELECT pg_catalog.pg_extension_config_dump(
       '_timescaledb_catalog.continuous_aggs_materialization_invalidation_log',
       '');

CREATE INDEX continuous_aggs_materialization_invalidation_log_idx ON
    _timescaledb_catalog.continuous_aggs_materialization_invalidation_log (
        materialization_id, lowest_modified_value ASC);
GRANT SELECT ON  _timescaledb_catalog.continuous_aggs_materialization_invalidation_log TO PUBLIC;

-- Suspend any running retention policies that conflict with continuous aggs
-- Note that this approach will work for both timestamp and integer time columns
DO $$
DECLARE
  jobid INTEGER;
BEGIN
    FOR jobid IN
        SELECT c.id
        FROM _timescaledb_config.bgw_job a
        LEFT JOIN _timescaledb_catalog.continuous_agg b ON a.hypertable_id = b.mat_hypertable_id
        INNER JOIN _timescaledb_config.bgw_job c ON c.hypertable_id = b.raw_hypertable_id
        WHERE a.proc_name = 'policy_refresh_continuous_aggregate' AND c.proc_name = 'policy_retention' AND c.scheduled
            AND ((a.config->'start_offset') = NULL OR (a.config->'start_offset')::text::interval > (c.config->'drop_after')::text::interval)
    LOOP
        RAISE NOTICE 'suspending data retention policy with job id %.', jobid
            USING DETAIL = 'The retention policy (formerly drop_chunks policy) will drop chunks while a continuous aggregate is still running on them. This will likely result in overwriting the aggregate with empty data.',
            HINT = ('To restore the retention policy, with the possibility of updating aggregates with dropped data, run: SELECT alter_job(%, scheduled=>true);  Otherwise, please create a new rention_policy with a larger drop_after parameter and remove the old policy with: SELECT delete_job(%);', jobid, jobid);
        UPDATE _timescaledb_config.bgw_job SET scheduled = false WHERE id = jobid;
    END LOOP;
END $$;
