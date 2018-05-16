\ir include/insert_two_partitions.sql

SELECT * FROM hypertable_relation_size('"public"."two_Partitions"');
SELECT * FROM hypertable_relation_size_pretty('"public"."two_Partitions"');
SELECT * FROM chunk_relation_size('"public"."two_Partitions"');
SELECT * FROM chunk_relation_size_pretty('"public"."two_Partitions"');
SELECT * FROM indexes_relation_size('"public"."two_Partitions"');
SELECT * FROM indexes_relation_size_pretty('"public"."two_Partitions"');

CREATE TABLE timestamp_partitioned(time TIMESTAMP, value TEXT);
SELECT * FROM create_hypertable('timestamp_partitioned', 'time', 'value', 2);

INSERT INTO timestamp_partitioned VALUES('2004-10-19 10:23:54', '10');
INSERT INTO timestamp_partitioned VALUES('2004-12-19 10:23:54', '30');
SELECT * FROM chunk_relation_size('timestamp_partitioned');
SELECT * FROM chunk_relation_size_pretty('timestamp_partitioned');


CREATE TABLE timestamp_partitioned_2(time TIMESTAMP, value CHAR(9));
SELECT * FROM create_hypertable('timestamp_partitioned_2', 'time', 'value', 2);

INSERT INTO timestamp_partitioned_2 VALUES('2004-10-19 10:23:54', '10');
INSERT INTO timestamp_partitioned_2 VALUES('2004-12-19 10:23:54', '30');
SELECT * FROM chunk_relation_size('timestamp_partitioned_2');
SELECT * FROM chunk_relation_size_pretty('timestamp_partitioned_2');

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
ANALYZE approx_count;

SELECT * FROM hypertable_approximate_row_count('approx_count');

-- all hypertables
SELECT * FROM hypertable_approximate_row_count();
