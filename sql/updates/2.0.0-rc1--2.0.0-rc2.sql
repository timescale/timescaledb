
DROP FUNCTION IF EXISTS @extschema@.detach_data_node(name,regclass,boolean,boolean);
DROP FUNCTION IF EXISTS @extschema@.distributed_exec;

DROP PROCEDURE IF EXISTS @extschema@.refresh_continuous_aggregate;

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

-- Recreate missing dimension slices that might be missing. If the
-- dimension slice table is broken and there are dimension slices
-- missing from the table, we will repair it by:
--
--    1. Finding all chunk constraints that have missing dimension
--       slices and extract the constraint expression from the
--       associated constraint.
--
--    2. Parse the constraint expression and extract the column name,
--       and upper and lower range values as text or, if it is a
--       partition constraint, pick the existing constraint (either
--       uppper or lower end of range) and make the other end open.
--
--    3. Use the column type to construct the range values (UNIX
--       microseconds) from these strings.
INSERT INTO _timescaledb_catalog.dimension_slice
WITH
   -- All dimension slices that are mentioned in the chunk_constraint
   -- table but are missing from the dimension_slice table.
   missing_slices AS (
      SELECT hypertable_id,
             chunk_id,
             dimension_slice_id,
             constraint_name,
             attname AS column_name,
             pg_get_expr(conbin, conrelid) AS constraint_expr
      FROM _timescaledb_catalog.chunk_constraint cc
      JOIN _timescaledb_catalog.chunk ch ON cc.chunk_id = ch.id
      JOIN pg_constraint ON conname = constraint_name
      JOIN pg_namespace ns ON connamespace = ns.oid AND ns.nspname = ch.schema_name
      JOIN pg_attribute ON attnum = conkey[1] AND attrelid = conrelid
      WHERE
         dimension_slice_id NOT IN (SELECT id FROM _timescaledb_catalog.dimension_slice)
   ),

  -- Unparsed range start and end for each dimension slice id that
  -- is missing.
   unparsed_missing_slices AS (
      SELECT di.id AS dimension_id,
             dimension_slice_id,
             constraint_name,
             column_type,
             column_name,
             (SELECT SUBSTRING(constraint_expr, $$>=\s*'?([\w\d\s:+-]+)'?$$)) AS range_start,
             (SELECT SUBSTRING(constraint_expr, $$<\s*'?([\w\d\s:+-]+)'?$$)) AS range_end
        FROM missing_slices JOIN _timescaledb_catalog.dimension di USING (hypertable_id, column_name)
   )
SELECT DISTINCT
       dimension_slice_id,
       dimension_id,
       CASE
       WHEN column_type = 'timestamptz'::regtype THEN
            EXTRACT(EPOCH FROM range_start::timestamptz)::bigint * 1000000
       WHEN column_type = 'timestamp'::regtype THEN
            EXTRACT(EPOCH FROM range_start::timestamp)::bigint * 1000000
       WHEN column_type = 'date'::regtype THEN
            EXTRACT(EPOCH FROM range_start::date)::bigint * 1000000
       ELSE
            CASE
            WHEN range_start IS NULL
            THEN (-9223372036854775808)::bigint
            ELSE range_start::bigint
            END
       END AS range_start,
       CASE
       WHEN column_type = 'timestamptz'::regtype THEN
            EXTRACT(EPOCH FROM range_end::timestamptz)::bigint * 1000000
       WHEN column_type = 'timestamp'::regtype THEN
            EXTRACT(EPOCH FROM range_end::timestamp)::bigint * 1000000
       WHEN column_type = 'date'::regtype THEN
            EXTRACT(EPOCH FROM range_end::date)::bigint * 1000000
       ELSE
            CASE WHEN range_end IS NULL
            THEN 9223372036854775807::bigint
            ELSE range_end::bigint
            END
       END AS range_end
  FROM unparsed_missing_slices;

CREATE FUNCTION @extschema@.detach_data_node(
    node_name              NAME,
    hypertable             REGCLASS = NULL,
    if_attached            BOOLEAN = FALSE,
    force                  BOOLEAN = FALSE,
    repartition            BOOLEAN = TRUE
) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_data_node_detach' LANGUAGE C VOLATILE;

CREATE OR REPLACE PROCEDURE @extschema@.distributed_exec(
       query TEXT,
       node_list name[] = NULL,
       transactional BOOLEAN = TRUE)
AS '@MODULE_PATHNAME@', 'ts_distributed_exec' LANGUAGE C;

