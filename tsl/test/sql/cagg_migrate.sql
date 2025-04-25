-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_CLUSTER_SUPERUSER

-- ########################################################
-- ## INTEGER data type tests
-- ########################################################
\set IS_TIME_DIMENSION FALSE
\set TIME_DIMENSION_DATATYPE INTEGER
\ir include/cagg_migrate_common.sql

-- ########################################################
-- ## TIMESTAMP data type tests
-- ########################################################
\set IS_TIME_DIMENSION TRUE
\set TIME_DIMENSION_DATATYPE TIMESTAMP
\ir include/cagg_migrate_common.sql

-- ########################################################
-- ## TIMESTAMPTZ data type tests
-- ########################################################
\set IS_TIME_DIMENSION TRUE
\set TIME_DIMENSION_DATATYPE TIMESTAMPTZ
\ir include/cagg_migrate_common.sql


-- #########################################################
-- Issue 5359 - custom timezones should not break the migration
-- #########################################################
SET timezone = 'Europe/Budapest';

-- Test with timestamp
\set TIME_DIMENSION_DATATYPE TIMESTAMP
\ir include/cagg_migrate_custom_timezone.sql

-- Test with timestamptz
\set TIME_DIMENSION_DATATYPE TIMESTAMPTZ
\ir include/cagg_migrate_custom_timezone.sql

-- #########################################################
-- Issue 6976 - space partitioning should not cause catalog corruption
-- #########################################################

CREATE TABLE space_partitioning (
	time timestamptz,
	device_id integer,
	value float
);

-- Updating sequence numbers so creating a hypertable doesn't mess with
-- data imports used by migration tests
SELECT setval('_timescaledb_catalog.hypertable_id_seq', 999, true);
SELECT setval('_timescaledb_catalog.chunk_id_seq', 999, true);
SELECT setval('_timescaledb_catalog.dimension_id_seq', 999, true);
SELECT setval('_timescaledb_catalog.dimension_slice_id_seq', 999, true);

SELECT create_hypertable('space_partitioning', 'time', chunk_time_interval=>'1 hour'::interval);
SELECT add_dimension('space_partitioning', 'device_id', 3);

INSERT INTO space_partitioning SELECT t, 1, 1.0 FROM generate_series('2024-01-01'::timestamptz, '2024-02-01'::timestamptz, '10 minutes'::interval) t;
INSERT INTO space_partitioning SELECT t, 1000, 1.0 FROM generate_series('2024-01-01'::timestamptz, '2024-02-01'::timestamptz, '10 minutes'::interval) t;

CREATE MATERIALIZED VIEW space_partitioning_summary
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
SELECT
    time_bucket(INTERVAL '1 week', "time") AS bucket,
    device_id,
    MIN(value),
    MAX(value),
    SUM(value)
FROM
    space_partitioning
GROUP BY
    1, 2
WITH NO DATA;

-- setting up the state so that remove_dropped_chunk_metadata
-- would run on the hypertable and trigger the catalog corruption
UPDATE _timescaledb_catalog.chunk
SET dropped = TRUE
FROM _timescaledb_catalog.hypertable
WHERE  chunk.hypertable_id = hypertable.id
AND hypertable.table_name = 'space_partitioning'
AND chunk.id = 1000;
UPDATE _timescaledb_catalog.continuous_agg
SET finalized = true
FROM _timescaledb_catalog.hypertable
WHERE continuous_agg.raw_hypertable_id = hypertable.id
AND hypertable.table_name = 'space_partitioning';
SET timescaledb.restoring TO ON;
DROP TABLE _timescaledb_internal._hyper_1000_1000_chunk;
SET timescaledb.restoring TO OFF;

SELECT _timescaledb_functions.remove_dropped_chunk_metadata(id)
FROM _timescaledb_catalog.hypertable
WHERE table_name = 'space_partitioning';


-- check every chunk has as many chunk constraints as
-- there are dimensions, should return empty result
-- this ensures we have avoided catalog corruption
WITH dimension_count as (
SELECT ht.id, count(*)
FROM _timescaledb_catalog.hypertable ht
INNER JOIN _timescaledb_catalog.dimension d
	ON d.hypertable_id = ht.id
WHERE table_name = 'space_partitioning'
GROUP BY 1),
chunk_constraint_count AS (
SELECT c.hypertable_id, cc.chunk_id, count(*)
FROM _timescaledb_catalog.chunk_constraint cc
INNER JOIN _timescaledb_catalog.chunk c
	ON cc.chunk_id = c.id
GROUP BY 1, 2
)
SELECT *
FROM dimension_count dc
INNER JOIN chunk_constraint_count ccc
	ON ccc.hypertable_id = dc.id
WHERE dc.count != ccc.count;

