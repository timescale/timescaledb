-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

--Enable compression path info
SET timescaledb.show_compression_path_info= 'on';
--Table creation
CREATE TABLE tab1 (
    time timestamptz not null,
    id integer not null,
    c1 double precision null,
    c2 double precision null
);

CREATE TABLE tab2 (
    time timestamptz not null,
    id integer not null,
    c1 double precision null,
    c2 double precision null
);
--Hypertable creation
SELECT FROM create_hypertable('tab1', 'time');
SELECT FROM create_hypertable('tab2', 'time');

--Data generation
INSERT INTO tab1
SELECT
time + (INTERVAL '1 minute' * random()) AS time,
id,
random() AS c1,
random()* 100 AS c2
FROM
generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-28 1:00', '1 hour') AS g1(time),
generate_series(1, 100, 1 ) AS g2(id)
ORDER BY
time;

--Test Set 1.1 [ Index(ASC, Null_First), Compression(ASC, Null_First) ]
CREATE INDEX idx_asc_null_first ON tab1(id, time ASC NULLS FIRST);
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id', timescaledb.compress_orderby = 'time NULLS FIRST');
SELECT * FROM timescaledb_information.compression_settings;
SELECT compress_chunk(show_chunks('tab1'));
--Cleanup 1.1
SELECT decompress_chunk(show_chunks('tab1'));

--Test Set 1.2 [Index(ASC, Null_First), Compression(ASC,Null_Last)]
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id', timescaledb.compress_orderby = 'time');
SELECT * FROM timescaledb_information.compression_settings;
SELECT compress_chunk(show_chunks('tab1'));
--Cleanup 1.2
SELECT decompress_chunk(show_chunks('tab1'));

--Test Set 1.3 [Index(ASC, Null_First), Compression(DESC,Null_First)]
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id', timescaledb.compress_orderby = 'time DESC');
SELECT * FROM timescaledb_information.compression_settings;
SELECT compress_chunk(show_chunks('tab1'));
--Cleanup 1.3
SELECT decompress_chunk(show_chunks('tab1'));
--DROP INDEX idx_asc_null_last

--Test Set 1.4 [Index(ASC, Null_First), Compression(DESC,Null_Last)]
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id', timescaledb.compress_orderby = 'time DESC NULLS LAST');
SELECT * FROM timescaledb_information.compression_settings;
SELECT compress_chunk(show_chunks('tab1'));
--Cleanup 1.4
SELECT decompress_chunk(show_chunks('tab1'));
DROP INDEX idx_asc_null_first;

--Test Set 2.1 [Index(ASC, Null_Last), Compression(ASC,Null_First)]
CREATE INDEX idx_asc_null_last ON tab1(id, time);
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id', timescaledb.compress_orderby = 'time NULLS FIRST');
SELECT * FROM timescaledb_information.compression_settings;
SELECT compress_chunk(show_chunks('tab1'));
--Cleanup 2.1
SELECT decompress_chunk(show_chunks('tab1'));

--Test Set 2.2 [Index(ASC, Null_Last), Compression(ASC,Null_Last)]
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id', timescaledb.compress_orderby = 'time');
SELECT * FROM timescaledb_information.compression_settings;
SELECT compress_chunk(show_chunks('tab1'));
--Cleanup 2.2
SELECT decompress_chunk(show_chunks('tab1'));

--Test Set 2.3 [Index(ASC, Null_Last), Compression(DESC,Null_First)]
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id', timescaledb.compress_orderby = 'time DESC');
SELECT * FROM timescaledb_information.compression_settings;
SELECT compress_chunk(show_chunks('tab1'));
--Cleanup 2.3
SELECT decompress_chunk(show_chunks('tab1'));
--DROP INDEX idx_asc_null_last

--Test Set 2.4 [Index(ASC, Null_Last), Compression(DESC,Null_Last)]
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id', timescaledb.compress_orderby = 'time DESC NULLS LAST');
SELECT * FROM timescaledb_information.compression_settings;
SELECT compress_chunk(show_chunks('tab1'));
--Cleanup 2.4
SELECT decompress_chunk(show_chunks('tab1'));
DROP INDEX idx_asc_null_last;

--Test Set 3.1 [Index(DESC, Null_First), Compression(ASC,Null_First)]
CREATE INDEX idx_desc_null_first ON tab1(id, time DESC NULLS FIRST);
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id', timescaledb.compress_orderby = 'time NULLS FIRST');
SELECT * FROM timescaledb_information.compression_settings;
SELECT compress_chunk(show_chunks('tab1'));
--Cleanup 3.1
SELECT decompress_chunk(show_chunks('tab1'));


--Test Set 3.2 [Index(DESC, Null_First), Compression(ASC,Null_Last)]
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id', timescaledb.compress_orderby = 'time');
SELECT * FROM timescaledb_information.compression_settings;
SELECT compress_chunk(show_chunks('tab1'));
--Cleanup 3.2
SELECT decompress_chunk(show_chunks('tab1'));

--Test Set 3.3 [Index(DESC, Null_First), Compression(DESC,Null_First)]
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id', timescaledb.compress_orderby = 'time DESC');
SELECT * FROM timescaledb_information.compression_settings;
SELECT compress_chunk(show_chunks('tab1'));
--Cleanup 3.3
SELECT decompress_chunk(show_chunks('tab1'));
--DROP INDEX idx_asc_null_last

--Test Set 3.4 [Index(DESC, Null_First), Compression(DESC,Null_Last)]
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id', timescaledb.compress_orderby = 'time DESC NULLS LAST');
SELECT * FROM timescaledb_information.compression_settings;
SELECT compress_chunk(show_chunks('tab1'));
--Cleanup 3.4
SELECT decompress_chunk(show_chunks('tab1'));
DROP INDEX idx_desc_null_first;

--Test Set 4.1 [Index(DESC, Null_Last), Compression(ASC,Null_First)]
CREATE INDEX idx_desc_null_last ON tab1(id, time DESC);
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id', timescaledb.compress_orderby = 'time NULLS FIRST');
SELECT * FROM timescaledb_information.compression_settings;
SELECT compress_chunk(show_chunks('tab1'));
--Cleanup 4.1
SELECT decompress_chunk(show_chunks('tab1'));

--Test Set 4.2 [Index(DESC, Null_Last), Compression(ASC,Null_Last)]
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id', timescaledb.compress_orderby = 'time');
SELECT * FROM timescaledb_information.compression_settings;
SELECT compress_chunk(show_chunks('tab1'));
--Cleanup 4.2
SELECT decompress_chunk(show_chunks('tab1'));

--Test Set 4.3 [Index(DESC, Null_Last), Compression(DESC,Null_First)]
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id', timescaledb.compress_orderby = 'time DESC');
SELECT * FROM timescaledb_information.compression_settings;
SELECT compress_chunk(show_chunks('tab1'));
--Cleanup 4.3
SELECT decompress_chunk(show_chunks('tab1'));

--Test Set 4.4 [Index(DESC, Null_Last), Compression(DESC,Null_Last)]
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id', timescaledb.compress_orderby = 'time DESC NULLS LAST');
SELECT * FROM timescaledb_information.compression_settings;
SELECT compress_chunk(show_chunks('tab1'));
SELECT decompress_chunk(show_chunks('tab1'));
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = '', timescaledb.compress_orderby = 'time');
SELECT compress_chunk(show_chunks('tab1'));
--Cleanup 4.4
SELECT decompress_chunk(show_chunks('tab1'));
DROP INDEX idx_desc_null_last;

--Test Set 5 GUC SET timescaledb.enable_compression_indexscan
-- Default this flag will be true.
SET timescaledb.enable_compression_indexscan = 'OFF';
SELECT compress_chunk(show_chunks('tab1'));
SELECT decompress_chunk(show_chunks('tab1'));
SET timescaledb.enable_compression_indexscan = 'ON';
SELECT compress_chunk(show_chunks('tab1'));
SELECT decompress_chunk(show_chunks('tab1'));

--Test Set 6 Compare two compression paths
INSERT into tab2 SELECT * from tab1;
CREATE INDEX idx_asc_null_first ON tab1(id, time ASC NULLS FIRST);
CREATE INDEX idx2_asc_null_first ON tab2(id, time ASC NULLS FIRST);
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id', timescaledb.compress_orderby = 'time NULLS FIRST');
ALTER TABLE tab2 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id', timescaledb.compress_orderby = 'time NULLS FIRST');
SET timescaledb.enable_compression_indexscan = 'OFF';
SELECT compress_chunk(show_chunks('tab1'));
SET timescaledb.enable_compression_indexscan = 'ON';
SELECT compress_chunk(show_chunks('tab2'));
SELECT id, time from tab1 EXCEPT SELECT id, time from tab2;
SELECT decompress_chunk(show_chunks('tab1'));
SELECT decompress_chunk(show_chunks('tab2'));
DROP INDEX idx2_asc_null_first;
DROP INDEX idx_asc_null_first;

--Test Set 7.1 with Collation order_by
\set ON_ERROR_STOP 1
CREATE TABLE tab3 (
    name text,
    time timestamptz not null
);

SELECT FROM create_hypertable('tab3', 'time');
--Data generation
INSERT INTO tab3
SELECT
name,
time + (INTERVAL '1 minute' * random()) AS time
FROM
md5(random()::text) as name,
generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-28 1:00', '1 hour') AS g1(time)
ORDER BY
time;
CREATE INDEX idx_asc_null_first ON tab3(name COLLATE "C", time ASC NULLS FIRST);
ALTER TABLE tab3 SET(timescaledb.compress, timescaledb.compress_segmentby = '', timescaledb.compress_orderby = 'name, time NULLS FIRST');
SELECT compress_chunk(show_chunks('tab3'));
SELECT decompress_chunk(show_chunks('tab3'));
CREATE INDEX idxcol_asc_null_first ON tab3(name, time ASC NULLS FIRST);
SELECT compress_chunk(show_chunks('tab3'));
SELECT decompress_chunk(show_chunks('tab3'));
DROP INDEX idx_asc_null_first;
DROP INDEX idxcol_asc_null_first;

--Test Set 7.1 with Collation segment_by
CREATE INDEX idx_asc_null_first ON tab3(name COLLATE "ucs_basic", time ASC NULLS FIRST);
ALTER TABLE tab3 SET(timescaledb.compress, timescaledb.compress_segmentby = 'name', timescaledb.compress_orderby = 'time NULLS FIRST');
SELECT compress_chunk(show_chunks('tab3'));
SELECT decompress_chunk(show_chunks('tab3'));
CREATE INDEX idxcol_asc_null_first ON tab3(name, time ASC NULLS FIRST);
SELECT compress_chunk(show_chunks('tab3'));
SELECT decompress_chunk(show_chunks('tab3'));
DROP INDEX idx_asc_null_first;
DROP INDEX idxcol_asc_null_first;

--Test Set 8 with multiple segment_by
CREATE INDEX idx_asc_null_first ON tab1(id, c1, time ASC NULLS FIRST);
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id, c1', timescaledb.compress_orderby = 'time NULLS FIRST');
SELECT compress_chunk(show_chunks('tab1'));
SELECT decompress_chunk(show_chunks('tab1'));
DROP INDEX idx_asc_null_first;
CREATE INDEX idx_asc_null_first ON tab1(id, c1 DESC, time ASC NULLS FIRST);
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id, c1', timescaledb.compress_orderby = 'time NULLS FIRST');
SELECT compress_chunk(show_chunks('tab1'));
SELECT decompress_chunk(show_chunks('tab1'));
DROP INDEX idx_asc_null_first;

--Test Set 9
--Last Column mismatch
CREATE INDEX idx_asc_null_first ON tab1(id, c1, c2);
ALTER TABLE tab1 SET(timescaledb.compress, timescaledb.compress_segmentby = 'id, c1', timescaledb.compress_orderby = 'time NULLS FIRST');
SELECT compress_chunk(show_chunks('tab1'));
SELECT decompress_chunk(show_chunks('tab1'));
DROP INDEX idx_asc_null_first;
--Index Column out of order
CREATE INDEX idx_asc_null_first ON tab1(c1, id, time ASC NULLS FIRST);
SELECT compress_chunk(show_chunks('tab1'));
SELECT decompress_chunk(show_chunks('tab1'));
DROP INDEX idx_asc_null_first;

--Tear down
DROP TABLE tab1;
DROP TABLE tab2;
DROP TABLE tab3;
SET timescaledb.show_compression_path_info = 'off';