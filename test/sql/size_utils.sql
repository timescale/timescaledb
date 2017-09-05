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
