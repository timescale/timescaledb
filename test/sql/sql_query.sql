\o /dev/null
\ir include/insert_two_partitions.sql
\o

SELECT * FROM PUBLIC."two_Partitions";

EXPLAIN (verbose ON, costs off) SELECT * FROM PUBLIC."two_Partitions";

\echo "The following queries should NOT scan two_Partitions._hyper_1_1_0_partition"
EXPLAIN (verbose ON, costs off) SELECT * FROM PUBLIC."two_Partitions" WHERE device_id = 'dev20';
EXPLAIN (verbose ON, costs off) SELECT * FROM PUBLIC."two_Partitions" WHERE device_id = 'dev'||'20';
EXPLAIN (verbose ON, costs off) SELECT * FROM PUBLIC."two_Partitions" WHERE 'dev'||'20' = device_id;

--TODO: handle this later?
--EXPLAIN (verbose ON, costs off) SELECT * FROM "two_Partitions" WHERE device_id IN ('dev20', 'dev21');

\echo "The following shows non-aggregated queries with time desc using merge append"
EXPLAIN (verbose ON, costs off)SELECT * FROM PUBLIC."two_Partitions" ORDER BY "timeCustom" DESC NULLS LAST limit 2;

--shows that more specific indexes are used if the WHERE clauses "match", uses the series_1 index here.
EXPLAIN (verbose ON, costs off)SELECT * FROM PUBLIC."two_Partitions" WHERE series_1 IS NOT NULL ORDER BY "timeCustom" DESC NULLS LAST limit 2;
--here the "match" is implication series_1 > 1 => series_1 IS NOT NULL
EXPLAIN (verbose ON, costs off)SELECT * FROM PUBLIC."two_Partitions" WHERE series_1 > 1 ORDER BY "timeCustom" DESC NULLS LAST limit 2;

--note that without time transform things work too
EXPLAIN (verbose ON, costs off)SELECT "timeCustom" t, min(series_0) FROM PUBLIC."two_Partitions" GROUP BY t ORDER BY t DESC NULLS LAST limit 2;

--TODO: time transform doesn't work
EXPLAIN (verbose ON, costs off)SELECT "timeCustom"/10 t, min(series_0) FROM PUBLIC."two_Partitions" GROUP BY t ORDER BY t DESC NULLS LAST limit 2;
EXPLAIN (verbose ON, costs off)SELECT "timeCustom"%10 t, min(series_0) FROM PUBLIC."two_Partitions" GROUP BY t ORDER BY t DESC NULLS LAST limit 2;


--make table with timestamp. Test timestamp instead of int time.
CREATE TABLE PUBLIC.hyper_1 (
  time TIMESTAMPTZ NOT NULL,
  series_0 DOUBLE PRECISION NULL,
  series_1 DOUBLE PRECISION NULL,
  series_2 DOUBLE PRECISION NULL
);

CREATE INDEX ON PUBLIC.hyper_1 (time DESC, series_0);
SELECT * FROM create_hypertable('"public"."hyper_1"'::regclass, 'time'::name, number_partitions => 1, chunk_size_bytes=>100000);
INSERT INTO hyper_1 SELECT to_timestamp(generate_series(0,10000)), random(), random(), random();

--non-aggragated uses MergeAppend correctly
EXPLAIN (verbose ON, costs off)SELECT * FROM hyper_1 ORDER BY "time" DESC limit 2;

--TODO: aggregated with date_trunc doesn't work
EXPLAIN (verbose ON, costs off)SELECT date_trunc('minute', time) t, min(series_0) FROM hyper_1 GROUP BY t ORDER BY t DESC limit 2;
