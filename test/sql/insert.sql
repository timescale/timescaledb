\ir include/insert_two_partitions.sql

\d+ "_timescaledb_internal".*
SELECT *
FROM "_timescaledb_internal"._hyper_1_0_replica;
SELECT * FROM _timescaledb_catalog.chunk;
SELECT * FROM _timescaledb_catalog.chunk_replica_node;
SELECT * FROM _timescaledb_catalog.partition_replica;

SELECT * FROM "two_Partitions";
SELECT * FROM ONLY "two_Partitions";

CREATE TABLE error_test(time timestamp, temp float8, device text NOT NULL);
SELECT create_hypertable('error_test', 'time', 'device', 2);

INSERT INTO error_test VALUES ('Mon Mar 20 09:18:20.1 2017', 21.3, 'dev1');
\set ON_ERROR_STOP 0
-- generate insert error 
INSERT INTO error_test VALUES ('Mon Mar 20 09:18:22.3 2017', 21.1, NULL);
\set ON_ERROR_STOP 1
INSERT INTO error_test VALUES ('Mon Mar 20 09:18:25.7 2017', 22.4, 'dev2');
SELECT * FROM error_test;
