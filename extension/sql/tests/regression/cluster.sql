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

CREATE TABLE PUBLIC."testNs2" (
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


SELECT * FROM create_hypertable('"public"."testNs"', 'time', 'Device_id');

\set ON_ERROR_STOP 0
SELECT * FROM create_hypertable('"public"."testNs"', 'time', 'Device_id');
\set ON_ERROR_STOP 1
SELECT * FROM create_hypertable('"public"."testNs2"', 'time', 'Device_id');

SELECT set_is_distinct_flag('"public"."testNs"', 'Device_id', TRUE);

\c meta
SELECT *
FROM _iobeamdb_catalog.partition_replica;

SELECT *
FROM _iobeamdb_meta.get_or_create_chunk(1, 1257894000000000000 :: BIGINT);

SELECT *
FROM _iobeamdb_catalog.node;
SELECT *
FROM _iobeamdb_catalog.meta;
SELECT *
FROM _iobeamdb_catalog.hypertable;
SELECT *
FROM _iobeamdb_catalog.hypertable_replica;
SELECT *
FROM _iobeamdb_catalog.distinct_replica_node;
SELECT *
FROM _iobeamdb_catalog.partition_epoch;
SELECT *
FROM _iobeamdb_catalog.partition;
SELECT *
FROM _iobeamdb_catalog.partition_replica;
SELECT *
FROM _iobeamdb_catalog.chunk;
SELECT *
FROM _iobeamdb_catalog.chunk_replica_node;
SELECT *
FROM _iobeamdb_catalog.hypertable_column;

\des+
\deu+

\echo *********************************************************************************************************ß
\c Test1

SELECT *
FROM _iobeamdb_catalog.node;
SELECT *
FROM _iobeamdb_catalog.meta;
SELECT *
FROM _iobeamdb_catalog.hypertable;
SELECT *
FROM _iobeamdb_catalog.hypertable_replica;
SELECT *
FROM _iobeamdb_catalog.distinct_replica_node;
SELECT *
FROM _iobeamdb_catalog.partition_epoch;
SELECT *
FROM _iobeamdb_catalog.partition;
SELECT *
FROM _iobeamdb_catalog.partition_replica;
SELECT *
FROM _iobeamdb_catalog.chunk;
SELECT *
FROM _iobeamdb_catalog.chunk_replica_node;
SELECT *
FROM _iobeamdb_catalog.hypertable_column;

\set ON_ERROR_STOP 0
UPDATE _iobeamdb_catalog.cluster_user SET password = 'foo';
UPDATE _iobeamdb_catalog.node SET active = FALSE;
DELETE FROM _iobeamdb_catalog.meta WHERE TRUE;
\set ON_ERROR_STOP 1

\des+
\deu+
\d+ "_iobeamdb_internal".*

--\d+ "_sys_1_testNs"."_sys_1_testNs_1_0_partition"
--\d+ "_sys_1_testNs"."_sys_1_testNs_2_0_partition"
--\det "_sys_1_testNs".*
--\d+ "testNs".distinct
--test idempotence
\c meta

SELECT *
FROM _iobeamdb_meta.get_or_create_chunk(1, 1257894000000000000 :: BIGINT);
\c Test1
\d+ "_iobeamdb_internal".*

\c meta
SELECT _iobeamdb_meta.close_chunk_end(1);
SELECT *
FROM _iobeamdb_catalog.chunk;

SELECT *
FROM _iobeamdb_meta.get_or_create_chunk(1, 10 :: BIGINT);
SELECT *
FROM _iobeamdb_meta.get_or_create_chunk(1, 1257894000000000000 :: BIGINT);
SELECT *
FROM _iobeamdb_catalog.chunk;

\c Test1
\d+ "_iobeamdb_internal".*
