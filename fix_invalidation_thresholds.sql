-- Script to fix invalidation thresholds and invalidation log entries
-- that have values near END_TIMESTAMP (> 9200000000000000000)
--
-- This script:
-- 1. Updates invalidation thresholds to the max watermark of their caggs
-- 2. Updates cagg's invalidation log entries to their corresponding cagg's watermark


-- Threshold for detecting problematic values
\set THRESHOLD 9200000000000000000
set search_path to public;

BEGIN;

\echo 'Acquiring locks on the tables to be modified or consulted'

-- Lock the catalog tables to prevent concurrent cagg refreshes from modifying them
-- EXCLUSIVE mode allows SELECTs but blocks all modifications (INSERT/UPDATE/DELETE)
-- This prevents any cagg refresh from:
-- 1. Updating invalidation thresholds
-- 2. Moving invalidations to/from the log
-- 3. Change cagg's watermark
LOCK TABLE _timescaledb_catalog.continuous_aggs_invalidation_threshold IN EXCLUSIVE MODE;
LOCK TABLE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log IN EXCLUSIVE MODE;
LOCK TABLE _timescaledb_catalog.continuous_aggs_watermark IN EXCLUSIVE MODE;

\echo 'Locks acquired successfully.'

-- Display current state before changes

\echo 'BEFORE: Problematic Invalidation Thresholds'

CREATE TABLE wrong_thresholds
AS
SELECT
    it.hypertable_id,
    h.schema_name || '.' || h.table_name as hypertable_name,
    it.watermark as current_threshold,
    COUNT(ca.mat_hypertable_id) as num_caggs
FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold it
JOIN _timescaledb_catalog.hypertable h ON h.id = it.hypertable_id
JOIN _timescaledb_catalog.continuous_agg ca ON ca.raw_hypertable_id = it.hypertable_id
WHERE it.watermark > :THRESHOLD
GROUP BY it.hypertable_id, h.schema_name, h.table_name, it.watermark
ORDER BY it.hypertable_id;

SELECT * FROM wrong_thresholds;


\echo 'BEFORE: Problematic Materialization Invalidation Log Entries'

CREATE TABLE wrong_invalidations
AS
SELECT
    il.materialization_id,
    ca.user_view_schema || '.' || ca.user_view_name as cagg_name,
    il.lowest_modified_value,
    il.greatest_modified_value,
    cw.watermark as cagg_watermark
FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log il
JOIN _timescaledb_catalog.continuous_agg ca ON ca.mat_hypertable_id = il.materialization_id
JOIN _timescaledb_catalog.continuous_aggs_watermark cw ON cw.mat_hypertable_id = il.materialization_id
WHERE il.lowest_modified_value > :THRESHOLD
ORDER BY il.materialization_id, il.lowest_modified_value;

SELECT * FROM wrong_invalidations;

\echo 'Check that there is no abnormal cagg watermark - should return 0'

SELECT count(*)
FROM _timescaledb_catalog.continuous_aggs_watermark
WHERE watermark > :THRESHOLD;


\echo 'Update invalidation threshold'

UPDATE _timescaledb_catalog.continuous_aggs_invalidation_threshold it
SET watermark = (
  SELECT MAX(cw.watermark)
  FROM _timescaledb_catalog.continuous_agg ca
  JOIN _timescaledb_catalog.continuous_aggs_watermark cw
      ON cw.mat_hypertable_id = ca.mat_hypertable_id
  WHERE ca.raw_hypertable_id = it.hypertable_id
)
WHERE watermark > :THRESHOLD;


\echo 'AFTER: No more problematic Invalidation Thresholds'
SELECT
    it.hypertable_id,
    h.schema_name || '.' || h.table_name as hypertable_name,
    it.watermark as current_threshold,
    COUNT(ca.mat_hypertable_id) as num_caggs
FROM _timescaledb_catalog.continuous_aggs_invalidation_threshold it
JOIN _timescaledb_catalog.hypertable h ON h.id = it.hypertable_id
JOIN _timescaledb_catalog.continuous_agg ca ON ca.raw_hypertable_id = it.hypertable_id
WHERE it.hypertable_id IN ( SELECT hypertable_id FROM wrong_thresholds)
GROUP BY it.hypertable_id, h.schema_name, h.table_name, it.watermark
ORDER BY it.hypertable_id;


\echo 'Update materialization ranges'

UPDATE _timescaledb_catalog.continuous_aggs_materialization_invalidation_log l
SET lowest_modified_value = (
    SELECT w.watermark
    FROM _timescaledb_catalog.continuous_aggs_watermark w
    WHERE w.mat_hypertable_id = l.materialization_id)
WHERE lowest_modified_value > :THRESHOLD;

\echo 'AFTER: no more wrong materialization invalidation logs'
SELECT
    il.materialization_id,
    ca.user_view_schema || '.' || ca.user_view_name as cagg_name,
    il.lowest_modified_value,
    il.greatest_modified_value,
    cw.watermark as cagg_watermark
FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log il
JOIN _timescaledb_catalog.continuous_agg ca ON ca.mat_hypertable_id = il.materialization_id
JOIN _timescaledb_catalog.continuous_aggs_watermark cw ON cw.mat_hypertable_id = il.materialization_id
JOIN wrong_invalidations wi ON il.materialization_id = wi.materialization_id 
                            AND il.greatest_modified_value = wi.greatest_modified_value
ORDER BY il.materialization_id, il.lowest_modified_value;

\echo ''
\echo '========================================='
\echo 'Summary'
\echo '========================================='
\echo 'Script completed. Review the changes above.'
\echo 'If everything looks correct, type COMMIT; to apply changes.'
\echo 'If something looks wrong, type ROLLBACK; to undo changes.'
\echo '========================================='

-- Don't auto-commit, let's review first
-- If everything looks OK:

-- COMMIT;
-- DROP TABLE wrong_thresholds cascade;
-- DROP TABLE wrong_invalidations casccade;
