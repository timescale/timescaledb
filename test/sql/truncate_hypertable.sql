\o /dev/null
\ir include/insert_two_partitions.sql
\o

SELECT * FROM _timescaledb_catalog.hypertable;
SELECT * FROM _timescaledb_catalog.chunk;
\dt "_timescaledb_internal".*
SELECT * FROM "two_Partitions";

SET client_min_messages = WARNING;
TRUNCATE "two_Partitions";

SELECT * FROM _timescaledb_catalog.hypertable;
SELECT * FROM _timescaledb_catalog.chunk;

-- should be empty
\set ON_ERROR_STOP 0
\dt "_timescaledb_internal".*
\set ON_ERROR_STOP 1

\d+ "two_Partitions"
SELECT * FROM "two_Partitions";


