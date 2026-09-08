-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

SET timescaledb.enable_direct_compress_insert TO ON;

-- Table with single segmentby and single orderby keys including NULL segmentby, unordered chunks
CREATE TABLE t2_seg(time int NOT NULL, dev int, v int);
SELECT table_name FROM create_hypertable('t2_seg', 'time', chunk_time_interval => 10000);
ALTER TABLE t2_seg SET (timescaledb.compress, timescaledb.compress_orderby='time DESC', timescaledb.compress_segmentby='dev');

INSERT INTO t2_seg (time, dev, v) values
(1, 1, 30),
(1, 2, 20),
(2, 3, 20),
(3, 1, 30),
(4, 1, 30),
(4, 2, 20),
(6, 3, 20),
(6, 1, 30),
(6, 2, 20),
(8, 1, 30),
(8, 2, 20),
(8, 3, 10),
(1, NULL, 5),
(6, NULL, 5),
(8, NULL, 5);

INSERT INTO t2_seg (time, dev, v) values
(3, 1, 30),
(3, 2, 20),
(3, 3, 20),
(5, 1, 30),
(5, 2, 20),
(5, 3, 10),
(6, 1, 30),
(6, 1, 30),
(6, 1, 30),
(7, 1, 30),
(7, 2, 20),
(7, 3, 10),
(3, NULL, 5),
(5, NULL, 5),
(7, NULL, 5);

INSERT INTO t2_seg (time, dev, v) values
(5, 1, 30),
(5, 3, 20),
(5, 2, 1),
(7, 2, 20),
(8, 1, 30),
(8, 2, 1),
(8, 3, 20),
(10, 1, 30),
(11, 1, 40),
(14, 1, 60),
(14, 2, 1),
(14, 3, 20),
(8, NULL, 5),
(5, NULL, 5),
(14, NULL, 5);

INSERT INTO t2_seg (time, dev, v) values
(9, 1, 30),
(9, 3, 300),
(9, 2, 20),
(9, 2, 10),
(10, 1, 30),
(10, 2, 10),
(10, 3, 20),
(11, 1, 30),
(12, 1, 300),
(13, 1, 60),
(13, 2, 10),
(13, 3, 70),
(9, NULL, 5),
(10, NULL, 5),
(13, NULL, 5);

SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('t2_seg') chunk;

-- Test with multi-segmentby, multi-orderby table, unordered chunks
CREATE TABLE t2_multi (
    time timestamptz NOT NULL,
    x1 integer,
    x2 text,
    x3 integer,
    x4 integer);

SELECT FROM create_hypertable('t2_multi', 'time');

ALTER TABLE t2_multi SET (timescaledb.compress, timescaledb.compress_segmentby='x1, x2', timescaledb.compress_orderby = 'time DESC, x3 ASC');

-- Segments (1,NULL), (1,3), (2,2), (2,4).
-- Batches (1hr,2)-(8hr,3), (3hr,2) - (7hr,1), (5hr,1)-(11hr,2)
-- Need to insert more than 10 tuples to use direct compress
INSERT INTO t2_multi (time, x1, x2, x3, x4) values
('2000-01-01 02:00:00-00', 1, NULL, 1, 10),
('2000-01-01 01:00:00-00', 1, NULL, 2, 10),
('2000-01-01 08:00:00-00', 1, NULL, 3, 10),
('2000-01-01 08:00:00-00', 1, '3', 2, 20),
('2000-01-01 04:00:00-00', 1, '3', 1, 20),
('2000-01-01 01:00:00-00', 1, '3', 2, 20),
('2000-01-01 04:00:00-00', 2, '2', 2, 30),
('2000-01-01 08:00:00-00', 2, '2', 3, 30),
('2000-01-01 01:00:00-00', 2, '2', 2, 30),
('2000-01-01 01:00:00-00', 2, '4', 2, 40),
('2000-01-01 02:00:00-00', 2, '4', 2, 40),
('2000-01-01 08:00:00-00', 2, '4', 3, 40);

INSERT INTO t2_multi (time, x1, x2, x3, x4) values
('2000-01-01 03:00:00-00', 1, NULL, 2, 10),
('2000-01-01 07:00:00-00', 1, NULL, 1, 10),
('2000-01-01 05:00:00-00', 1, NULL, 1, 10),
('2000-01-01 04:00:00-00', 1, '3', 1, 20),
('2000-01-01 03:00:00-00', 1, '3', 3, 20),
('2000-01-01 07:00:00-00', 1, '3', 1, 20),
('2000-01-01 03:00:00-00', 2, '2', 2, 30),
('2000-01-01 03:00:00-00', 2, '2', 5, 30),
('2000-01-01 07:00:00-00', 2, '2', 1, 30),
('2000-01-01 03:00:00-00', 2, '4', 3, 40),
('2000-01-01 06:00:00-00', 2, '4', 3, 40),
('2000-01-01 07:00:00-00', 2, '4', 1, 40);

INSERT INTO t2_multi (time, x1, x2, x3, x4) values
('2000-01-01 05:00:00-00', 1, NULL, 2, 10),
('2000-01-01 07:00:00-00', 1, NULL, 1, 10),
('2000-01-01 11:00:00-00', 1, NULL, 2, 10),
('2000-01-01 05:00:00-00', 1, '3', 1, 20),
('2000-01-01 08:00:00-00', 1, '3', 3, 20),
('2000-01-01 11:00:00-00', 1, '3', 1, 20),
('2000-01-01 05:00:00-00', 2, '2', 2, 30),
('2000-01-01 06:00:00-00', 2, '2', 5, 30),
('2000-01-01 11:00:00-00', 2, '2', 1, 30),
('2000-01-01 05:00:00-00', 2, '4', 4, 40),
('2000-01-01 10:00:00-00', 2, '4', 3, 40),
('2000-01-01 11:00:00-00', 2, '4', 2, 40);

SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('t2_multi') chunk;

SET timescaledb.enable_direct_compress_insert TO OFF;

-- Test with multi-segmentby, multi-orderby table, ordered chunks
CREATE TABLE t2_multi_reg (
    time timestamptz NOT NULL,
    x1 integer,
    x2 text,
    x3 integer,
    x4 integer);

SELECT FROM create_hypertable('t2_multi_reg', 'time');

ALTER TABLE t2_multi_reg SET (timescaledb.compress, timescaledb.compress_segmentby='x1, x2', timescaledb.compress_orderby = 'time DESC, x3 ASC');

INSERT into t2_multi_reg SELECT * from t2_multi;
SELECT count(compress_chunk(ch)) FROM show_chunks('t2_multi_reg') ch;

SELECT _timescaledb_functions.chunk_status_text(chunk) FROM show_chunks('t2_multi_reg') chunk;

RESET timescaledb.enable_direct_compress_insert;
