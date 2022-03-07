-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\ir include/insert_two_partitions.sql

SELECT * FROM hypertable_detailed_size('"public"."two_Partitions"');
SELECT * FROM hypertable_index_size('"public"."two_Partitions_device_id_timeCustom_idx"');
SELECT * FROM hypertable_index_size('"public"."two_Partitions_timeCustom_device_id_idx"');
SELECT * FROM hypertable_index_size('"public"."two_Partitions_timeCustom_idx"');
SELECT * FROM hypertable_index_size('"public"."two_Partitions_timeCustom_series_0_idx"');
SELECT * FROM hypertable_index_size('"public"."two_Partitions_timeCustom_series_1_idx"');
SELECT * FROM hypertable_index_size('"public"."two_Partitions_timeCustom_series_2_idx"');
SELECT * FROM hypertable_index_size('"public"."two_Partitions_timeCustom_series_bool_idx"');
SELECT * FROM chunks_detailed_size('"public"."two_Partitions"') order by chunk_name;

CREATE TABLE timestamp_partitioned(time TIMESTAMP, value TEXT);
SELECT * FROM create_hypertable('timestamp_partitioned', 'time', 'value', 2);

INSERT INTO timestamp_partitioned VALUES('2004-10-19 10:23:54', '10');
INSERT INTO timestamp_partitioned VALUES('2004-12-19 10:23:54', '30');
SELECT * FROM chunks_detailed_size('timestamp_partitioned') order by chunk_name;

CREATE TABLE timestamp_partitioned_2(time TIMESTAMP, value CHAR(9));
SELECT * FROM create_hypertable('timestamp_partitioned_2', 'time', 'value', 2);

INSERT INTO timestamp_partitioned_2 VALUES('2004-10-19 10:23:54', '10');
INSERT INTO timestamp_partitioned_2 VALUES('2004-12-19 10:23:54', '30');
SELECT * FROM chunks_detailed_size('timestamp_partitioned_2') order by chunk_name;

CREATE TABLE toast_test(time TIMESTAMP, value TEXT);
-- Set storage type to EXTERNAL to prevent PostgreSQL from compressing my
-- easily compressable string and instead store it with TOAST
ALTER TABLE toast_test ALTER COLUMN value SET STORAGE EXTERNAL;
SELECT * FROM create_hypertable('toast_test', 'time');

INSERT INTO toast_test VALUES('2004-10-19 10:23:54', $$
this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k. this must be over 2k.
$$);
SELECT * FROM chunks_detailed_size('toast_test');

--
-- Tests for approximate_row_count()
--

-- Regular table
--
CREATE TABLE approx_count(time TIMESTAMP, value int);
INSERT INTO approx_count VALUES('2004-01-01 10:00:01', 1);
INSERT INTO approx_count VALUES('2004-01-01 10:00:02', 2);
INSERT INTO approx_count VALUES('2004-01-01 10:00:03', 3);
INSERT INTO approx_count VALUES('2004-01-01 10:00:04', 4);
INSERT INTO approx_count VALUES('2004-01-01 10:00:05', 5);
INSERT INTO approx_count VALUES('2004-01-01 10:00:06', 6);
INSERT INTO approx_count VALUES('2004-01-01 10:00:07', 7);
SELECT * FROM approximate_row_count('approx_count');
ANALYZE approx_count;
SELECT count(*) FROM approx_count;
SELECT * FROM approximate_row_count('approx_count');
DROP TABLE approx_count;

-- Regular table with basic inheritance
--
CREATE TABLE approx_count(id int);
CREATE TABLE approx_count_child(id2 int) INHERITS (approx_count);
INSERT INTO approx_count_child VALUES(0);
INSERT INTO approx_count VALUES(1);
SELECT count(*) FROM approx_count;
SELECT * FROM approximate_row_count('approx_count');
ANALYZE approx_count;
SELECT * FROM approximate_row_count('approx_count');
ANALYZE approx_count_child;
SELECT * FROM approximate_row_count('approx_count');
DROP TABLE approx_count CASCADE;

-- Regular table with nested inheritance
--
CREATE TABLE approx_count(id int);
CREATE TABLE approx_count_a(id2 int) INHERITS (approx_count);
CREATE TABLE approx_count_b(id3 int) INHERITS (approx_count_a);
CREATE TABLE approx_count_c(id4 int) INHERITS (approx_count_b);

INSERT INTO approx_count_a VALUES(0);
INSERT INTO approx_count_b VALUES(1);
INSERT INTO approx_count_c VALUES(2);
INSERT INTO approx_count VALUES(3);

SELECT * FROM approximate_row_count('approx_count');

ANALYZE approx_count_a;
ANALYZE approx_count_b;
ANALYZE approx_count_c;
ANALYZE approx_count;

SELECT count(*) FROM approx_count;
SELECT * FROM approximate_row_count('approx_count');
SELECT count(*) FROM approx_count_a;
SELECT * FROM approximate_row_count('approx_count_a');
SELECT count(*) FROM approx_count_b;
SELECT * FROM approximate_row_count('approx_count_b');
SELECT count(*) FROM approx_count_c;
SELECT * FROM approximate_row_count('approx_count_c');

DROP TABLE approx_count CASCADE;

-- table with declarative partitioning
--

CREATE TABLE approx_count_dp(time TIMESTAMP, value int) PARTITION BY RANGE(time);

CREATE TABLE approx_count_dp0 PARTITION OF approx_count_dp
FOR VALUES FROM ('2004-01-01 00:00:00') TO ('2005-01-01 00:00:00');
CREATE TABLE approx_count_dp1 PARTITION OF approx_count_dp
FOR VALUES FROM ('2005-01-01 00:00:00') TO ('2006-01-01 00:00:00');
CREATE TABLE approx_count_dp2 PARTITION OF approx_count_dp
FOR VALUES FROM ('2006-01-01 00:00:00') TO ('2007-01-01 00:00:00');

INSERT INTO approx_count_dp VALUES('2004-01-01 10:00:00', 1);
INSERT INTO approx_count_dp VALUES('2004-01-01 11:00:00', 1);
INSERT INTO approx_count_dp VALUES('2004-01-01 12:00:01', 1);

INSERT INTO approx_count_dp VALUES('2005-01-01 10:00:00', 1);
INSERT INTO approx_count_dp VALUES('2005-01-01 11:00:00', 1);
INSERT INTO approx_count_dp VALUES('2005-01-01 12:00:01', 1);

INSERT INTO approx_count_dp VALUES('2006-01-01 10:00:00', 1);
INSERT INTO approx_count_dp VALUES('2006-01-01 11:00:00', 1);
INSERT INTO approx_count_dp VALUES('2006-01-01 12:00:01', 1);

SELECT count(*) FROM approx_count_dp;
SELECT count(*) FROM approx_count_dp0;
SELECT count(*) FROM approx_count_dp1;
SELECT count(*) FROM approx_count_dp2;

SELECT * FROM approximate_row_count('approx_count_dp');
ANALYZE approx_count_dp;
SELECT * FROM approximate_row_count('approx_count_dp');
SELECT * FROM approximate_row_count('approx_count_dp0');
SELECT * FROM approximate_row_count('approx_count_dp1');
SELECT * FROM approximate_row_count('approx_count_dp2');

CREATE TABLE approx_count_dp_nested(time TIMESTAMP, device_id int, value int) PARTITION BY RANGE(time);
CREATE TABLE approx_count_dp_nested_0 PARTITION OF approx_count_dp_nested FOR VALUES FROM ('2004-01-01 00:00:00') TO ('2005-01-01 00:00:00') PARTITION BY RANGE (device_id);
CREATE TABLE approx_count_dp_nested_0_0 PARTITION OF approx_count_dp_nested_0 FOR VALUES FROM (0) TO (10);
CREATE TABLE approx_count_dp_nested_0_1 PARTITION OF approx_count_dp_nested_0 FOR VALUES FROM (10) TO (20);
CREATE TABLE approx_count_dp_nested_1 PARTITION OF approx_count_dp_nested FOR VALUES FROM ('2005-01-01 00:00:00') TO ('2006-01-01 00:00:00') PARTITION BY RANGE (device_id);
CREATE TABLE approx_count_dp_nested_1_0 PARTITION OF approx_count_dp_nested_1 FOR VALUES FROM (0) TO (10);
CREATE TABLE approx_count_dp_nested_1_1 PARTITION OF approx_count_dp_nested_1 FOR VALUES FROM (10) TO (20);

INSERT INTO approx_count_dp_nested VALUES('2004-01-01 10:00:00', 1, 1);
INSERT INTO approx_count_dp_nested VALUES('2004-01-01 10:00:00', 2, 1);
INSERT INTO approx_count_dp_nested VALUES('2004-01-01 10:00:00', 3, 1);
INSERT INTO approx_count_dp_nested VALUES('2004-01-01 10:00:00', 11, 1);
INSERT INTO approx_count_dp_nested VALUES('2004-01-01 10:00:00', 12, 1);
INSERT INTO approx_count_dp_nested VALUES('2004-01-01 10:00:00', 13, 1);
INSERT INTO approx_count_dp_nested VALUES('2005-01-01 10:00:00', 1, 1);
INSERT INTO approx_count_dp_nested VALUES('2005-01-01 10:00:00', 2, 1);
INSERT INTO approx_count_dp_nested VALUES('2005-01-01 10:00:00', 3, 1);
INSERT INTO approx_count_dp_nested VALUES('2005-01-01 10:00:00', 11, 1);
INSERT INTO approx_count_dp_nested VALUES('2005-01-01 10:00:00', 12, 1);
INSERT INTO approx_count_dp_nested VALUES('2005-01-01 10:00:00', 13, 1);

SELECT * FROM approximate_row_count('approx_count_dp_nested');
ANALYZE approx_count_dp_nested;

SELECT
  (SELECT count(*) FROM approx_count_dp_nested) AS dp_nested,
  (SELECT count(*) FROM approx_count_dp_nested_0) AS dp_nested_0,
  (SELECT count(*) FROM approx_count_dp_nested_0_0) AS dp_nested_0_0,
  (SELECT count(*) FROM approx_count_dp_nested_0_1) AS dp_nested_0_1,
  (SELECT count(*) FROM approx_count_dp_nested_1) AS dp_nested_1,
  (SELECT count(*) FROM approx_count_dp_nested_1_0) AS dp_nested_1_0,
  (SELECT count(*) FROM approx_count_dp_nested_1_1) AS dp_nested_1_1
UNION ALL
SELECT
  approximate_row_count('approx_count_dp_nested'),
  approximate_row_count('approx_count_dp_nested_0'),
  approximate_row_count('approx_count_dp_nested_0_0'),
  approximate_row_count('approx_count_dp_nested_0_1'),
  approximate_row_count('approx_count_dp_nested_1'),
  approximate_row_count('approx_count_dp_nested_1_0'),
  approximate_row_count('approx_count_dp_nested_1_1');

-- Hypertable
--
CREATE TABLE approx_count(time TIMESTAMP, value int);
SELECT * FROM create_hypertable('approx_count', 'time');
INSERT INTO approx_count VALUES('2004-01-01 10:00:01', 1);
INSERT INTO approx_count VALUES('2004-01-01 10:00:02', 2);
INSERT INTO approx_count VALUES('2004-01-01 10:00:03', 3);
INSERT INTO approx_count VALUES('2004-01-01 10:00:04', 4);
INSERT INTO approx_count VALUES('2004-01-01 10:00:05', 5);
INSERT INTO approx_count VALUES('2004-01-01 10:00:06', 6);
INSERT INTO approx_count VALUES('2004-01-01 10:00:07', 7);
INSERT INTO approx_count VALUES('2004-01-01 10:00:08', 8);
INSERT INTO approx_count VALUES('2004-01-01 10:00:09', 9);
INSERT INTO approx_count VALUES('2004-01-01 10:00:10', 10);
SELECT count(*) FROM approx_count;
SELECT * FROM approximate_row_count('approx_count');
ANALYZE approx_count;
SELECT * FROM approximate_row_count('approx_count');

\set ON_ERROR_STOP 0
SELECT * FROM approximate_row_count('unexisting');
SELECT * FROM approximate_row_count();
SELECT * FROM approximate_row_count(NULL);
\set ON_ERROR_STOP 1

-- Test size functions with invalid or non-existing OID
SELECT * FROM hypertable_size(0);
SELECT * FROM hypertable_detailed_size(0) ORDER BY node_name;
SELECT * FROM chunks_detailed_size(0) ORDER BY node_name;
SELECT * FROM hypertable_compression_stats(0) ORDER BY node_name;
SELECT * FROM chunk_compression_stats(0) ORDER BY node_name;
SELECT * FROM hypertable_index_size(0);
SELECT * FROM _timescaledb_internal.relation_size(0);

SELECT * FROM hypertable_size(1);
SELECT * FROM hypertable_detailed_size(1) ORDER BY node_name;
SELECT * FROM chunks_detailed_size(1) ORDER BY node_name;
SELECT * FROM hypertable_compression_stats(1) ORDER BY node_name;
SELECT * FROM chunk_compression_stats(1) ORDER BY node_name;
SELECT * FROM hypertable_index_size(1);
SELECT * FROM _timescaledb_internal.relation_size(1);

-- Test size functions with NULL input
SELECT * FROM hypertable_size(NULL);
SELECT * FROM hypertable_detailed_size(NULL) ORDER BY node_name;
SELECT * FROM chunks_detailed_size(NULL) ORDER BY node_name;
SELECT * FROM hypertable_compression_stats(NULL) ORDER BY node_name;
SELECT * FROM chunk_compression_stats(NULL) ORDER BY node_name;
SELECT * FROM hypertable_index_size(NULL);
SELECT * FROM _timescaledb_internal.relation_size(NULL);

-- Test size functions on regular table
CREATE TABLE hypersize(time timestamptz, device int);
CREATE INDEX hypersize_time_idx ON hypersize (time);
\set ON_ERROR_STOP 0
\set VERBOSITY default
\set SHOW_CONTEXT never
SELECT pg_relation_size('hypersize'), pg_table_size('hypersize'), pg_indexes_size('hypersize'), pg_total_relation_size('hypersize'), pg_relation_size('hypersize_time_idx');
SELECT * FROM _timescaledb_internal.relation_size('hypersize');
SELECT * FROM hypertable_size('hypersize');
SELECT * FROM hypertable_detailed_size('hypersize') ORDER BY node_name;
SELECT * FROM chunks_detailed_size('hypersize') ORDER BY node_name;
SELECT * FROM hypertable_compression_stats('hypersize') ORDER BY node_name;
SELECT * FROM chunk_compression_stats('hypersize') ORDER BY node_name;
SELECT * FROM hypertable_index_size('hypersize_time_idx');
\set VERBOSITY terse
\set ON_ERROR_STOP 1

-- Test size functions on empty hypertable
SELECT * FROM create_hypertable('hypersize', 'time');
SELECT pg_relation_size('hypersize'), pg_table_size('hypersize'), pg_indexes_size('hypersize'), pg_total_relation_size('hypersize'), pg_relation_size('hypersize_time_idx');
SELECT * FROM _timescaledb_internal.relation_size('hypersize');
SELECT * FROM hypertable_size('hypersize');
SELECT * FROM hypertable_detailed_size('hypersize') ORDER BY node_name;
SELECT * FROM chunks_detailed_size('hypersize') ORDER BY node_name;
SELECT * FROM hypertable_compression_stats('hypersize') ORDER BY node_name;
SELECT * FROM chunk_compression_stats('hypersize') ORDER BY node_name;
SELECT * FROM hypertable_index_size('hypersize_time_idx');

-- Test size functions on non-empty hypertable
INSERT INTO hypersize VALUES('2021-02-25', 1);
SELECT pg_relation_size('hypersize'), pg_table_size('hypersize'), pg_indexes_size('hypersize'), pg_total_relation_size('hypersize'), pg_relation_size('hypersize_time_idx');
SELECT pg_relation_size(ch), pg_table_size(ch), pg_indexes_size(ch), pg_total_relation_size(ch)
FROM show_chunks('hypersize') ch
ORDER BY ch;
SELECT * FROM show_chunks('hypersize') ch JOIN LATERAL _timescaledb_internal.relation_size(ch) ON true;
SELECT * FROM hypertable_size('hypersize');
SELECT * FROM hypertable_detailed_size('hypersize') ORDER BY node_name;
SELECT * FROM chunks_detailed_size('hypersize') ORDER BY node_name;
SELECT * FROM hypertable_compression_stats('hypersize') ORDER BY node_name;
SELECT * FROM chunk_compression_stats('hypersize') ORDER BY node_name;
SELECT * FROM hypertable_index_size('hypersize_time_idx');
