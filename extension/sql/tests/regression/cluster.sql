\set ON_ERROR_STOP 1
\set VERBOSITY verbose
\set SHOW_CONTEXT never

\ir include/create_clustered_db.sql

\set ECHO ALL
\c meta
SELECT add_cluster_user('postgres', NULL);

SELECT set_meta('meta' :: NAME, 'localhost');
SELECT add_node('Test1' :: NAME, 'localhost');

SELECT add_node('test2' :: NAME, 'localhost');

\c Test1

CREATE TABLE PUBLIC."testNs" (
  time BIGINT NOT NULL,
  "Device_id" TEXT NOT NULL,
  temp DOUBLE PRECISION NULL,
  occupied BOOLEAN NULL,
  latitude BIGINT NULL,
  really_long_column_goes_on_and_on_and_on_and_on_and_on_and_on_and_on_and_on BIGINT NULL
);

CREATE INDEX ON PUBLIC."testNs" ("Device_id", time DESC NULLS LAST) WHERE "Device_id" IS NOT NULL;
CREATE INDEX ON PUBLIC."testNs" (temp, time DESC NULLS LAST) WHERE temp IS NOT NULL;
CREATE INDEX ON PUBLIC."testNs" (really_long_column_goes_on_and_on_and_on_and_on_and_on_and_on_and_on_and_on, time DESC NULLS LAST) WHERE really_long_column_goes_on_and_on_and_on_and_on_and_on_and_on_and_on_and_on IS NOT NULL;
CREATE INDEX ON PUBLIC."testNs" (time DESC NULLS LAST, really_long_column_goes_on_and_on_and_on_and_on_and_on_and_on_and_on_and_on) WHERE really_long_column_goes_on_and_on_and_on_and_on_and_on_and_on_and_on_and_on IS NOT NULL;


SELECT * FROM create_hypertable('"public"."testNs"', 'time', 'Device_id', hypertable_name=>'testNs');

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('"public"."testNs"', 'time', 'Device_id', hypertable_name=>'testNs');
\set ON_ERROR_STOP 1

SELECT set_is_distinct_flag('"public"."testNs"', 'Device_id', TRUE);

\c meta
SELECT *
FROM partition_replica;

SELECT *
FROM _meta.get_or_create_chunk(1, 1257894000000000000 :: BIGINT);

SELECT *
FROM node;
SELECT *
FROM meta;
SELECT *
FROM hypertable;
SELECT *
FROM hypertable_replica;
SELECT *
FROM distinct_replica_node;
SELECT *
FROM partition_epoch;
SELECT *
FROM partition;
SELECT *
FROM partition_replica;
SELECT *
FROM chunk;
SELECT *
FROM chunk_replica_node;
SELECT *
FROM hypertable_column;

\des+
\deu+

\echo *********************************************************************************************************ß
\c Test1

SELECT *
FROM node;
SELECT *
FROM meta;
SELECT *
FROM hypertable;
SELECT *
FROM hypertable_replica;
SELECT *
FROM distinct_replica_node;
SELECT *
FROM partition_epoch;
SELECT *
FROM partition;
SELECT *
FROM partition_replica;
SELECT *
FROM chunk;
SELECT *
FROM chunk_replica_node;
SELECT *
FROM hypertable_column;

\des+
\deu+
\d+ "_sysinternal".*

--\d+ "_sys_1_testNs"."_sys_1_testNs_1_0_partition"
--\d+ "_sys_1_testNs"."_sys_1_testNs_2_0_partition"
--\det "_sys_1_testNs".*
--\d+ "testNs".distinct
--test idempotence
\c meta

SELECT *
FROM _meta.get_or_create_chunk(1, 1257894000000000000 :: BIGINT);
\c Test1
\d+ "_sysinternal".*

\c meta
SELECT _meta.close_chunk_end(1);
SELECT *
FROM chunk;

SELECT *
FROM _meta.get_or_create_chunk(1, 10 :: BIGINT);
SELECT *
FROM _meta.get_or_create_chunk(1, 1257894000000000000 :: BIGINT);
SELECT *
FROM chunk;

\c Test1
\d+ "_sysinternal".*
