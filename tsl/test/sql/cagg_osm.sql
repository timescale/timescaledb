-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--  These tests work for PG14 or greater
-- Remember to coordinate any changes to functionality with the Cloud
-- Native Storage team. Tests for the following API:
-- * cagg refresh with GUC enable_tiered_reads

\set EXPLAIN 'EXPLAIN (COSTS OFF)'

--SETUP for OSM chunk --
--need superuser access to create foreign data server
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE DATABASE postgres_fdw_db;
GRANT ALL PRIVILEGES ON DATABASE postgres_fdw_db TO :ROLE_4;

\c postgres_fdw_db :ROLE_4
CREATE TABLE fdw_table( timec timestamptz NOT NULL , acq_id bigint, value bigint);
INSERT INTO fdw_table VALUES( '2020-01-01 01:00', 100, 1000);

--create foreign server and user mappings as superuser
\c :TEST_DBNAME :ROLE_SUPERUSER

SELECT current_setting('port') as "PORTNO" \gset

CREATE EXTENSION postgres_fdw;
CREATE SERVER s3_server FOREIGN DATA WRAPPER postgres_fdw
OPTIONS ( host 'localhost', dbname 'postgres_fdw_db', port :'PORTNO');
GRANT USAGE ON FOREIGN SERVER s3_server TO :ROLE_4;

CREATE USER MAPPING FOR :ROLE_4 SERVER s3_server
OPTIONS (  user :'ROLE_4' , password :'ROLE_4_PASS');

ALTER USER MAPPING FOR :ROLE_4 SERVER s3_server
OPTIONS (ADD password_required 'false');

\c :TEST_DBNAME :ROLE_4;
-- this is a stand-in for the OSM table
CREATE FOREIGN TABLE child_fdw_table
(timec timestamptz NOT NULL, acq_id bigint, value bigint)
 SERVER s3_server OPTIONS ( schema_name 'public', table_name 'fdw_table');

--now attach foreign table as a chunk of the hypertable.
CREATE TABLE ht_try(timec timestamptz NOT NULL, acq_id bigint, value bigint);
SELECT create_hypertable('ht_try', 'timec', chunk_time_interval => interval '1 day');
INSERT INTO ht_try VALUES ('2022-05-05 01:00', 222, 222);

SELECT * FROM child_fdw_table;
SELECT _timescaledb_functions.attach_osm_table_chunk('ht_try', 'child_fdw_table');
-- must also update the range since the created chunk contains data
SELECT _timescaledb_functions.hypertable_osm_range_update('ht_try', '2020-01-01'::timestamptz, '2020-01-02');

set timescaledb.enable_tiered_reads = true;
SELECT * from ht_try ORDER BY 1;
-- IMPORTANT we test with disabled tiered reads
-- This set of tests verify that refresh_continuous_aggregate( cagg, NULL, ...) does not 
-- generate bad invalidation logs i.e.  invlaidations are for ranges that strictly visible
-- to the refresh
-- we are able to handle this only for the special case : refresh with start_time = NULL
set timescaledb.enable_tiered_reads = false;
SELECT * from ht_try ORDER BY 1;

--TEST cagg creation
CREATE MATERIALIZED VIEW cagg_ht_osm WITH (timescaledb.continuous, timescaledb.materialized_only = true)
AS
SELECT time_bucket('7 days'::interval, timec), count(*)
FROM ht_try
GROUP BY 1;

SELECT * FROM cagg_ht_osm ORDER BY 1;

SELECT * FROM 
_timescaledb_catalog.continuous_aggs_materialization_invalidation_log ORDER BY 1;

-- now refresh everything. it should still not change invalidation_log for tiered portion because 
-- tiered reads are off
CALL refresh_continuous_aggregate('cagg_ht_osm',  NULL, '2023-04-01');
SELECT * FROM cagg_ht_osm ORDER BY 1;
SELECT * FROM 
_timescaledb_catalog.continuous_aggs_materialization_invalidation_log ORDER BY 1;

DROP MATERIALIZED VIEW cagg_ht_osm;
-- TEST : Repeat cagg refresh test but without initially materializing any data
CREATE MATERIALIZED VIEW cagg_ht_osm WITH (timescaledb.continuous, timescaledb.materialized_only = true)
AS
SELECT time_bucket('7 days'::interval, timec), count(*)
FROM ht_try
GROUP BY 1 WITH NO DATA;
SELECT * FROM 
_timescaledb_catalog.continuous_aggs_materialization_invalidation_log ORDER BY 1;
CALL refresh_continuous_aggregate('cagg_ht_osm',  NULL, '2023-04-01');
SELECT * FROM cagg_ht_osm ORDER BY 1;
SELECT * FROM 
_timescaledb_catalog.continuous_aggs_materialization_invalidation_log ORDER BY 1;

DROP MATERIALIZED VIEW cagg_ht_osm;

-- TEST : cagg refresh test for variable size bucket
CREATE MATERIALIZED VIEW cagg_ht_osm_var WITH (timescaledb.continuous, timescaledb.materialized_only = true)
AS
SELECT time_bucket('1 month'::interval, timec), count(*)
FROM ht_try
GROUP BY 1 WITH NO DATA;
SELECT * FROM 
_timescaledb_catalog.continuous_aggs_materialization_invalidation_log ORDER BY 1;
CALL refresh_continuous_aggregate('cagg_ht_osm_var',  NULL, '2023-04-01');
SELECT * FROM cagg_ht_osm_var ORDER BY 1;
SELECT * FROM 
_timescaledb_catalog.continuous_aggs_materialization_invalidation_log ORDER BY 1;

DROP MATERIALIZED VIEW cagg_ht_osm_var;

