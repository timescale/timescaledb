
DROP FUNCTION IF EXISTS detach_data_node(name,regclass,boolean,boolean);
DROP FUNCTION IF EXISTS distributed_exec;
DROP FUNCTION IF EXISTS create_hypertable;
DROP FUNCTION IF EXISTS create_distributed_hypertable;

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
