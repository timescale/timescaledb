-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Test that continuous aggregate refresh with NULL start caps the
-- refresh window at the earliest chunk when tiered data is present
-- and tiered reads are disabled.

\c :TEST_DBNAME :ROLE_SUPERUSER
SELECT _timescaledb_functions.stop_background_workers();
SET datestyle TO 'ISO, YMD';
SET timezone TO 'UTC';

----------------------------------------------------------------------
-- Set up postgres_fdw for creating a real OSM chunk
----------------------------------------------------------------------
CREATE DATABASE osm_fdw_db;
GRANT ALL PRIVILEGES ON DATABASE osm_fdw_db TO :ROLE_4;

\c osm_fdw_db :ROLE_4
CREATE TABLE osm_data (time bigint NOT NULL, device int, temp float);
-- This represents tiered data that lives in object storage
INSERT INTO osm_data VALUES (-5, 1, 10.0), (-3, 2, 20.0);

\c :TEST_DBNAME :ROLE_SUPERUSER

SELECT current_setting('port') AS "PORTNO" \gset

CREATE EXTENSION postgres_fdw;
CREATE SERVER osm_server FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (dbname 'osm_fdw_db', port :'PORTNO');
GRANT USAGE ON FOREIGN SERVER osm_server TO :ROLE_4;

CREATE USER MAPPING FOR :ROLE_4 SERVER osm_server
OPTIONS (user :'ROLE_4', password :'ROLE_4_PASS');

ALTER USER MAPPING FOR :ROLE_4 SERVER osm_server
OPTIONS (ADD password_required 'false');

\c :TEST_DBNAME :ROLE_4

CREATE FOREIGN TABLE osm_fdw_chunk (time bigint NOT NULL, device int, temp float)
SERVER osm_server OPTIONS (schema_name 'public', table_name 'osm_data');

----------------------------------------------------------------------
-- Create hypertable and continuous aggregate
----------------------------------------------------------------------
CREATE TABLE conditions (time bigint NOT NULL, device int, temp float);
SELECT create_hypertable('conditions', 'time', chunk_time_interval => 10);

CREATE OR REPLACE FUNCTION bigint_now()
RETURNS bigint LANGUAGE SQL STABLE AS $$
  SELECT coalesce(max(time), 0) FROM conditions
$$;
SELECT set_integer_now_func('conditions', 'bigint_now');

-- Insert data from 0 to 99
INSERT INTO conditions
SELECT t, (t % 4) + 1, (t * 7) % 40
FROM generate_series(0, 99, 1) t;

CREATE MATERIALIZED VIEW cond_10
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT time_bucket(BIGINT '10', time) AS bucket, device, avg(temp) AS avg_temp
FROM conditions
GROUP BY 1,2 WITH NO DATA;

-- Invalidation log view
CREATE VIEW cagg_invals AS
SELECT materialization_id AS "cagg_id",
       lowest_modified_value AS "start",
       greatest_modified_value AS "end"
FROM _timescaledb_catalog.continuous_aggs_materialization_invalidation_log
ORDER BY 1,2,3;

-- Refresh to populate the cagg and establish the invalidation threshold
CALL refresh_continuous_aggregate('cond_10', 0, 100);

SELECT mat_hypertable_id AS cond_10_id
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name = 'cond_10' \gset

-- After refresh [0,100), the cagg log has sentinel entries for
-- the regions before and after the refreshed window.
SELECT "start", "end" FROM cagg_invals WHERE cagg_id = :cond_10_id;

----------------------------------------------------------------------
-- Attach OSM chunk to simulate tiered data
----------------------------------------------------------------------
SELECT _timescaledb_functions.attach_osm_table_chunk('conditions', 'osm_fdw_chunk');

-- Verify the hypertable now has OSM status
SELECT status FROM _timescaledb_catalog.hypertable WHERE table_name = 'conditions';

-- Verify earliest non-OSM chunk starts at 0
SELECT min(ds.range_start) AS earliest_start
FROM _timescaledb_catalog.chunk c
JOIN _timescaledb_catalog.chunk_constraint cc ON cc.chunk_id = c.id
JOIN _timescaledb_catalog.dimension_slice ds ON ds.id = cc.dimension_slice_id
WHERE c.hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'conditions')
  AND c.osm_chunk = false;

----------------------------------------------------------------------
-- Test 1: With tiered reads disabled and OSM chunk present, a
-- NULL-start refresh caps at the earliest chunk. The sentinel entry
-- (-INF, -1) is preserved because it falls entirely before the cap.
----------------------------------------------------------------------

-- Insert data to create invalidations that will trigger the refresh
INSERT INTO conditions VALUES (15, 1, 99.0), (25, 2, 99.0);

SET timescaledb.enable_tiered_reads = false;
CALL refresh_continuous_aggregate('cond_10', NULL, NULL);

-- The (-INF, -1) entry should be preserved
SELECT "start", "end" FROM cagg_invals WHERE cagg_id = :cond_10_id;

----------------------------------------------------------------------
-- Test 2: With tiered reads enabled, the cap is NOT applied.
-- The full refresh window is used.
----------------------------------------------------------------------
SET timescaledb.enable_tiered_reads = true;

-- Insert to create a new invalidation so the refresh has work to do
INSERT INTO conditions VALUES (35, 3, 99.0);

CALL refresh_continuous_aggregate('cond_10', NULL, NULL);

-- The (-INF, -1) entry should now be consumed
SELECT "start", "end" FROM cagg_invals WHERE cagg_id = :cond_10_id;

----------------------------------------------------------------------
-- Test 3: Verify the cap preserves pre-chunk invalidations for a
-- hypertable whose data does not start at 0.
----------------------------------------------------------------------
DROP MATERIALIZED VIEW cond_10;
DROP TABLE conditions CASCADE;

-- Create a fresh hypertable with data starting at 20
CREATE TABLE conditions2 (time bigint NOT NULL, device int, temp float);
SELECT create_hypertable('conditions2', 'time', chunk_time_interval => 10);

CREATE OR REPLACE FUNCTION bigint_now2()
RETURNS bigint LANGUAGE SQL STABLE AS $$
  SELECT coalesce(max(time), 0) FROM conditions2
$$;
SELECT set_integer_now_func('conditions2', 'bigint_now2');

INSERT INTO conditions2
SELECT t, (t % 4) + 1, (t * 7) % 40
FROM generate_series(20, 79, 1) t;

CREATE MATERIALIZED VIEW cond2_10
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
SELECT time_bucket(BIGINT '10', time) AS bucket, device, avg(temp) AS avg_temp
FROM conditions2
GROUP BY 1,2 WITH NO DATA;

CALL refresh_continuous_aggregate('cond2_10', 20, 80);

SELECT mat_hypertable_id AS cond2_10_id
FROM _timescaledb_catalog.continuous_agg
WHERE user_view_name = 'cond2_10' \gset

-- The invalidation log should have entries for before 20 and after 80
SELECT "start", "end" FROM cagg_invals WHERE cagg_id = :cond2_10_id;

-- Attach a new OSM chunk for this hypertable
CREATE FOREIGN TABLE osm_fdw_chunk2 (time bigint NOT NULL, device int, temp float)
SERVER osm_server OPTIONS (schema_name 'public', table_name 'osm_data');

SELECT _timescaledb_functions.attach_osm_table_chunk('conditions2', 'osm_fdw_chunk2');

-- Verify earliest non-OSM chunk starts at 20
SELECT min(ds.range_start) AS earliest_start
FROM _timescaledb_catalog.chunk c
JOIN _timescaledb_catalog.chunk_constraint cc ON cc.chunk_id = c.id
JOIN _timescaledb_catalog.dimension_slice ds ON ds.id = cc.dimension_slice_id
WHERE c.hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'conditions2')
  AND c.osm_chunk = false;

-- Insert to create invalidation
INSERT INTO conditions2 VALUES (35, 3, 99.0);

SET timescaledb.enable_tiered_reads = false;
CALL refresh_continuous_aggregate('cond2_10', NULL, NULL);

-- The (-INF, 19) entry should be preserved because the cap is at 20
SELECT "start", "end" FROM cagg_invals WHERE cagg_id = :cond2_10_id;

----------------------------------------------------------------------
-- Test 4: The cap also applies when an explicit start is given that
-- falls before the earliest chunk. A refresh with start=0 should
-- still be capped at 20 when tiered reads are disabled.
----------------------------------------------------------------------

-- Insert to create a new invalidation
INSERT INTO conditions2 VALUES (45, 2, 99.0);

-- Explicit start of 0, which is before the earliest chunk at 20
CALL refresh_continuous_aggregate('cond2_10', 0, 80);

-- The (-INF, 19) entry should still be preserved because the cap
-- raises the effective start from 0 to 20
SELECT "start", "end" FROM cagg_invals WHERE cagg_id = :cond2_10_id;

----------------------------------------------------------------------
-- Test 5: When the OSM chunk's range is updated to precede the
-- earliest real chunk the refresh should be capped correctly.
----------------------------------------------------------------------

-- Update the OSM chunk range so it sorts before all real chunks
SELECT _timescaledb_functions.hypertable_osm_range_update('conditions2', -10::bigint, 10::bigint);

-- Verify the OSM chunk now has a range that precedes the real data
SELECT ds.range_start, ds.range_end, c.osm_chunk
FROM _timescaledb_catalog.chunk c
JOIN _timescaledb_catalog.chunk_constraint cc ON cc.chunk_id = c.id
JOIN _timescaledb_catalog.dimension_slice ds ON ds.id = cc.dimension_slice_id
WHERE c.hypertable_id = (SELECT id FROM _timescaledb_catalog.hypertable WHERE table_name = 'conditions2')
ORDER BY ds.range_start;

-- Insert to create a new invalidation
INSERT INTO conditions2 VALUES (55, 1, 99.0);

SET timescaledb.enable_tiered_reads = false;
CALL refresh_continuous_aggregate('cond2_10', NULL, NULL);

-- The refresh cap skips the OSM chunk entry and caps at the earliest hypertable entry
SELECT "start", "end" FROM cagg_invals WHERE cagg_id = :cond2_10_id;

----------------------------------------------------------------------
-- Cleanup
----------------------------------------------------------------------
SET timescaledb.enable_tiered_reads = true;
DROP MATERIALIZED VIEW cond2_10;
DROP TABLE conditions2 CASCADE;
DROP VIEW cagg_invals;

\c :TEST_DBNAME :ROLE_SUPERUSER
DROP SERVER osm_server CASCADE;
DROP EXTENSION postgres_fdw;
DROP DATABASE osm_fdw_db;
