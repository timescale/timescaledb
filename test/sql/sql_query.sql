-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\o /dev/null
\ir include/insert_two_partitions.sql
\o

SELECT * FROM PUBLIC."two_Partitions";

EXPLAIN (verbose ON, buffers off, costs off) SELECT * FROM PUBLIC."two_Partitions";

\echo "The following queries should NOT scan two_Partitions._hyper_1_1_chunk"
EXPLAIN (verbose ON, buffers off, costs off) SELECT * FROM PUBLIC."two_Partitions" WHERE device_id = 'dev2';
EXPLAIN (verbose ON, buffers off, costs off) SELECT * FROM PUBLIC."two_Partitions" WHERE device_id = 'dev'||'2';
EXPLAIN (verbose ON, buffers off, costs off) SELECT * FROM PUBLIC."two_Partitions" WHERE 'dev'||'2' = device_id;

--test integer partition key
CREATE TABLE "int_part"(time timestamp, object_id int, temp float);
SELECT create_hypertable('"int_part"', 'time', 'object_id', 2);
INSERT INTO "int_part" VALUES('2017-01-20T09:00:01', 1, 22.5);
INSERT INTO "int_part" VALUES('2017-01-20T09:00:01', 2, 22.5);

--check that there are two chunks
SELECT * FROM test.show_subtables('int_part');

SELECT * FROM "int_part" WHERE object_id = 1;

--check that queries with IN/ANY/= work for the "time" column
SELECT * FROM "int_part" WHERE time IN (NULL);
SELECT * FROM "int_part" WHERE time = ANY (NULL);
SELECT * FROM "int_part" WHERE time = NULL;

--make sure this touches only one partititon
EXPLAIN (verbose ON, buffers off, costs off) SELECT * FROM "int_part" WHERE object_id = 1;

--Need to verify space partitions are currently pruned in this query
--EXPLAIN (verbose ON, buffers off, costs off) SELECT * FROM "two_Partitions" WHERE device_id IN ('dev2', 'dev21');

\echo "The following shows non-aggregated queries with time desc using merge append"
EXPLAIN (verbose ON, buffers off, costs off)SELECT * FROM PUBLIC."two_Partitions" ORDER BY "timeCustom" DESC NULLS LAST limit 2;

--shows that more specific indexes are used if the WHERE clauses "match", uses the series_1 index here.
EXPLAIN (verbose ON, buffers off, costs off)SELECT * FROM PUBLIC."two_Partitions" WHERE series_1 IS NOT NULL ORDER BY "timeCustom" DESC NULLS LAST limit 2;
--here the "match" is implication series_1 > 1 => series_1 IS NOT NULL
EXPLAIN (verbose ON, buffers off, costs off)SELECT * FROM PUBLIC."two_Partitions" WHERE series_1 > 1 ORDER BY "timeCustom" DESC NULLS LAST limit 2;

--note that without time transform things work too
EXPLAIN (verbose ON, buffers off, costs off)SELECT "timeCustom" t, min(series_0) FROM PUBLIC."two_Partitions" GROUP BY t ORDER BY t DESC NULLS LAST limit 2;

--The query should still use the index on timeCustom, even though the GROUP BY/ORDER BY is on the transformed time 't'.
--However, current query plans show that it does not.
EXPLAIN (verbose ON, buffers off, costs off)SELECT "timeCustom"/10 t, min(series_0) FROM PUBLIC."two_Partitions" GROUP BY t ORDER BY t DESC NULLS LAST limit 2;
EXPLAIN (verbose ON, buffers off, costs off)SELECT "timeCustom"%10 t, min(series_0) FROM PUBLIC."two_Partitions" GROUP BY t ORDER BY t DESC NULLS LAST limit 2;
