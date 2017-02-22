\set ON_ERROR_STOP 1

\o /dev/null
\ir include/insert_single.sql

\o
\set ECHO ALL
\c single

SELECT * FROM PUBLIC."testNs";

EXPLAIN (verbose ON, costs off) SELECT * FROM PUBLIC."testNs";

\echo "The following queries should NOT scan testNs._hyper_1_1_0_partition"
EXPLAIN (verbose ON, costs off) SELECT * FROM PUBLIC."testNs" WHERE device_id = 'dev20';
EXPLAIN (verbose ON, costs off) SELECT * FROM PUBLIC."testNs" WHERE device_id = 'dev'||'20';
EXPLAIN (verbose ON, costs off) SELECT * FROM PUBLIC."testNs" WHERE 'dev'||'20' = device_id;

--TODO: handle this later?
--EXPLAIN (verbose ON, costs off) SELECT * FROM "testNs" WHERE device_id IN ('dev20', 'dev21');

