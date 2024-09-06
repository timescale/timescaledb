-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- TESTS for hypertable_osm_range_update_function
--           orderedappend (needs hypertable_osm_range_update to update catalog range)
--                         (so that ordered append will work with OSM chunks)
-- TEST for hypertable_osm_range_update function
-- test range of dimension slice for osm chunk for different datatypes
CREATE TABLE osm_int2(time int2 NOT NULL);
CREATE TABLE osm_int4(time int4 NOT NULL);
CREATE TABLE osm_int8(time int8 NOT NULL);
CREATE TABLE osm_date(time date NOT NULL);
CREATE TABLE osm_ts(time timestamp NOT NULL);
CREATE TABLE osm_tstz(time timestamptz NOT NULL);

SELECT table_name FROM create_hypertable('osm_int2','time',chunk_time_interval:=1000);
SELECT table_name FROM create_hypertable('osm_int4','time',chunk_time_interval:=1000);
SELECT table_name FROM create_hypertable('osm_int8','time',chunk_time_interval:=1000);
SELECT table_name FROM create_hypertable('osm_date','time');
SELECT table_name FROM create_hypertable('osm_ts','time');
SELECT table_name FROM create_hypertable('osm_tstz','time');

CREATE FOREIGN TABLE osm_int2_fdw_child(time int2 NOT NULL) SERVER s3_server;
CREATE FOREIGN TABLE osm_int4_fdw_child(time int4 NOT NULL) SERVER s3_server;
CREATE FOREIGN TABLE osm_int8_fdw_child(time int8 NOT NULL) SERVER s3_server;
CREATE FOREIGN TABLE osm_date_fdw_child(time date NOT NULL) SERVER s3_server;
CREATE FOREIGN TABLE osm_ts_fdw_child(time timestamp NOT NULL) SERVER s3_server;
CREATE FOREIGN TABLE osm_tstz_fdw_child(time timestamptz NOT NULL) SERVER s3_server;

SELECT dt, _timescaledb_functions.attach_osm_table_chunk('osm_' || dt, 'osm_' || dt || '_fdw_child') FROM unnest('{int2,int4,int8,date,ts,tstz}'::text[]) u(dt);

SELECT ht.table_name, ds.*
FROM _timescaledb_catalog.dimension_slice ds
INNER JOIN _timescaledb_catalog.dimension d ON d.id=ds.dimension_id
INNER JOIN _timescaledb_catalog.hypertable ht on ht.id=d.hypertable_id
WHERE ht.table_name LIKE 'osm%'
ORDER BY 2,3;
-- test that correct slice is found and updated for table with multiple chunk constraints
CREATE TABLE test_multicon(time timestamptz not null unique, a int);
SELECT hypertable_id as htid FROM create_hypertable('test_multicon', 'time', chunk_time_interval => interval '1 day') \gset
insert into test_multicon values ('2020-01-02 01:00'::timestamptz, 1);
SELECT c.id, c.hypertable_id, c.schema_name, c.table_name, c.compressed_chunk_id, c.dropped, c.status, c.osm_chunk,
cc.chunk_id, cc.dimension_slice_id, cc.constraint_name, cc.hypertable_constraint_name FROM
_timescaledb_catalog.chunk c, _timescaledb_catalog.chunk_constraint cc WHERE c.hypertable_id = :htid
AND c.id = cc.chunk_id;
\c :TEST_DBNAME :ROLE_SUPERUSER ;
UPDATE _timescaledb_catalog.chunk SET osm_chunk = true WHERE hypertable_id = :htid;
\c :TEST_DBNAME :ROLE_4;
SELECT _timescaledb_functions.hypertable_osm_range_update('test_multicon', '2020-01-02 01:00'::timestamptz, '2020-01-04 01:00');
-- view udpated range
SELECT cc.chunk_id, c.table_name, c.status, c.osm_chunk, cc.dimension_slice_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.chunk_constraint cc, _timescaledb_catalog.dimension_slice ds
WHERE c.hypertable_id = :htid AND cc.chunk_id = c.id AND ds.id = cc.dimension_slice_id;
-- check that range was reset to default - infinity
\set ON_ERROR_STOP 0
-- both range_start and range_end must be NULL, or non-NULL
SELECT _timescaledb_functions.hypertable_osm_range_update('test_multicon', NULL, '2020-01-04 01:00'::timestamptz);
SELECT _timescaledb_functions.hypertable_osm_range_update('test_multicon', NULL, NULL);
SELECT _timescaledb_functions.hypertable_osm_range_update('test_multicon');
\set ON_ERROR_STOP 1
SELECT _timescaledb_functions.hypertable_osm_range_update('test_multicon', NULL::timestamptz, NULL);
SELECT cc.chunk_id, c.table_name, c.status, c.osm_chunk, cc.dimension_slice_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.chunk_constraint cc, _timescaledb_catalog.dimension_slice ds
WHERE c.hypertable_id = :htid AND cc.chunk_id = c.id AND ds.id = cc.dimension_slice_id ORDER BY cc.chunk_id;

-- TEST for orderedappend that depends on hypertable_osm_range_update functionality
-- test further with ordered append
\c postgres_fdw_db :ROLE_4;
CREATE TABLE test_chunkapp_fdw (time timestamptz NOT NULL, a int);
INSERT INTO test_chunkapp_fdw (time, a) VALUES ('2020-01-03 02:00'::timestamptz, 3);

\c :TEST_DBNAME :ROLE_4
CREATE TABLE test_chunkapp(time timestamptz NOT NULL, a int);
SELECT hypertable_id as htid FROM create_hypertable('test_chunkapp', 'time', chunk_time_interval => interval '1day') \gset
INSERT INTO test_chunkapp (time, a) VALUES ('2020-01-01 01:00'::timestamptz, 1), ('2020-01-02 01:00'::timestamptz, 2);

CREATE FOREIGN TABLE test_chunkapp_fdw_child(time timestamptz NOT NULL, a int) SERVER s3_server OPTIONS (schema_name 'public', table_name 'test_chunkapp_fdw');;
SELECT _timescaledb_functions.attach_osm_table_chunk('test_chunkapp','test_chunkapp_fdw_child');
-- view range before update
SELECT cc.chunk_id, c.table_name, c.status, c.osm_chunk, cc.dimension_slice_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.chunk_constraint cc, _timescaledb_catalog.dimension_slice ds
WHERE c.hypertable_id = :htid AND cc.chunk_id = c.id AND ds.id = cc.dimension_slice_id ORDER BY cc.chunk_id;
-- attempt to update overlapping range, should fail
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.hypertable_osm_range_update('test_chunkapp', '2020-01-02 01:00'::timestamptz, '2020-01-04 01:00');
\set ON_ERROR_STOP 1
-- update actual range of OSM chunk, should work
SELECT _timescaledb_functions.hypertable_osm_range_update('test_chunkapp', '2020-01-03 00:00'::timestamptz, '2020-01-04 00:00');
-- view udpated range
SELECT cc.chunk_id, c.table_name, c.status, c.osm_chunk, cc.dimension_slice_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.chunk_constraint cc, _timescaledb_catalog.dimension_slice ds
WHERE c.hypertable_id = :htid AND cc.chunk_id = c.id AND ds.id = cc.dimension_slice_id ORDER BY cc.chunk_id;
-- ordered append should be possible as ranges do not overlap
:EXPLAIN SELECT * FROM test_chunkapp ORDER BY 1;
SELECT * FROM test_chunkapp ORDER BY 1;
-- but, insert should not be possible
SELECT ts_setup_osm_hook();
\set ON_ERROR_STOP 0
INSERT INTO test_chunkapp VALUES ('2020-01-03 02:00'::timestamptz, 3);
\set ON_ERROR_STOP 1
SELECT ts_undo_osm_hook();
-- reset range to infinity
SELECT _timescaledb_functions.hypertable_osm_range_update('test_chunkapp',empty:=false);
-- ordered append not possible because range is invalid and empty was not specified
:EXPLAIN SELECT * FROM test_chunkapp ORDER BY 1;
SELECT * FROM test_chunkapp ORDER BY 1;
SELECT cc.chunk_id, c.table_name, c.status, c.osm_chunk, cc.dimension_slice_id, ds.range_start, ds.range_end
FROM _timescaledb_catalog.chunk c, _timescaledb_catalog.chunk_constraint cc, _timescaledb_catalog.dimension_slice ds
WHERE c.hypertable_id = :htid AND cc.chunk_id = c.id AND ds.id = cc.dimension_slice_id ORDER BY cc.chunk_id;
-- but also, OSM chunk should be included in the scan, since range is invalid and chunk is not empty
:EXPLAIN SELECT * FROM test_chunkapp WHERE time < '2023-01-01' ORDER BY 1;
SELECT * FROM test_chunkapp WHERE time < '2023-01-01' ORDER BY 1;
-- now set empty to true, should ordered append
\c postgres_fdw_db :ROLE_4;
DELETE FROM test_chunkapp_fdw;
\c :TEST_DBNAME :ROLE_4;
SELECT _timescaledb_functions.hypertable_osm_range_update('test_chunkapp', NULL::timestamptz, NULL, empty => true);
:EXPLAIN SELECT * FROM test_chunkapp ORDER BY 1;
SELECT * FROM test_chunkapp ORDER BY 1;
-- should exclude the OSM chunk this time since it is empty
:EXPLAIN SELECT * FROM test_chunkapp WHERE time < '2023-01-01' ORDER BY 1;
SELECT * FROM test_chunkapp WHERE time < '2023-01-01' ORDER BY 1;

\set ON_ERROR_STOP 0
-- test adding constraint directly on OSM chunk is blocked
ALTER TABLE test_chunkapp_fdw_child ADD CHECK (a > 0); -- non-dimensional
ALTER TABLE test_chunkapp_fdw_child ADD CHECK (time > '1600-01-01'::timestamptz); -- dimensional
-- but on hypertable, it is allowed
ALTER TABLE test_chunkapp ADD CHECK (a > 0);
\d+ test_chunkapp_fdw_child
\set ON_ERROR_STOP 1

-- test error is triggered when time dimension not found
CREATE TABLE test2(time timestamptz not null, a int);
SELECT create_hypertable('test2', 'time');
INSERT INTO test2 VALUES ('2020-01-01'::timestamptz, 1);
ALTER TABLE test2 SET (timescaledb.compress);
SELECT compress_chunk(show_chunks('test2'));
-- find internal compression table, call API function on it
SELECT format('%I.%I', cht.schema_name, cht.table_name) AS "COMPRESSION_TBLNM"
FROM _timescaledb_catalog.hypertable ht, _timescaledb_catalog.hypertable cht
WHERE ht.table_name = 'test2' and cht.id = ht.compressed_hypertable_id \gset
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.hypertable_osm_range_update(:'COMPRESSION_TBLNM'::regclass, '2020-01-01'::timestamptz);
\set ON_ERROR_STOP 1

-- test wrong/incompatible data types with hypertable time dimension
-- update range of int2 with int4
\set ON_ERROR_STOP 0
SELECT _timescaledb_functions.hypertable_osm_range_update('osm_int2', range_start => 65540::int4, range_end => 100000::int4);
-- update range of int8 with int4
SELECT _timescaledb_functions.hypertable_osm_range_update('osm_int8', 120, 150);
-- update range of timestamptz with date
SELECT _timescaledb_functions.hypertable_osm_range_update('osm_tstz', '2020-01-01'::date, '2020-01-03'::date);
-- udpate range of timestamp with bigint
SELECT _timescaledb_functions.hypertable_osm_range_update('osm_tstz', 9223372036854771806, 9223372036854775406);
\set ON_ERROR_STOP 1

-- test dimension slice tuple visibility
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE TABLE osm_slice_update(time int not null);
SELECT hypertable_id AS ht_id FROM create_hypertable('osm_slice_update', 'time', chunk_time_interval => 10) \gset

INSERT INTO osm_slice_update VALUES (1);
UPDATE _timescaledb_catalog.hypertable SET status = 3 WHERE id = :ht_id;
UPDATE _timescaledb_catalog.chunk SET osm_chunk = true WHERE hypertable_id = :ht_id;

\c
BEGIN;
 	SELECT _timescaledb_functions.hypertable_osm_range_update('osm_slice_update',40,50);
ROLLBACK;

\c
-- new session should not be affected by previous rolled back transaction
-- should show 0 10 as range
\set ON_ERROR_STOP 0
INSERT INTO osm_slice_update VALUES (1);
INSERT INTO osm_slice_update VALUES (1);
\set ON_ERROR_STOP 1
