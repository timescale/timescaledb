-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

set work_mem to '256MB';

-- disable memoize on PG14+
SELECT CASE WHEN current_setting('server_version_num')::int/10000 >= 14 THEN set_config('enable_memoize','off',false) ELSE 'off' END AS enable_memoize;
SET enable_indexscan TO false;

-- test join on compressed time column
-- #3079, #4465
CREATE TABLE compressed_join_temp AS SELECT * FROM metrics ORDER BY time DESC LIMIT 10;
ANALYZE compressed_join_temp;

EXPLAIN (analyze,costs off,timing off,summary off) SELECT *
FROM compressed_join_temp t
INNER JOIN metrics_compressed m ON t.time = m.time AND t.device_id = m.device_id
ORDER BY t.device_id, t.time
LIMIT 1;

DROP TABLE compressed_join_temp;

-- test join with partially compressed chunks
CREATE TABLE partial_join(time timestamptz,device text);

SELECT table_name FROM create_hypertable('partial_join','time');
ALTER TABLE partial_join set(timescaledb.compress,timescaledb.compress_segmentby='device');

INSERT INTO partial_join SELECT '2000-01-01','d1';

SELECT count(*) FROM (SELECT compress_chunk(show_chunks('partial_join'),true)) compress;

-- make chunk partially compressed
INSERT INTO partial_join SELECT '2000-01-01','d1';

SELECT * FROM partial_join m1 INNER JOIN partial_join m2 ON m1.device = m2.device;

DROP TABLE partial_join;

-- This resulted in a recursion in the join planner code on PG16
SELECT * FROM metrics_tstz as m
INNER JOIN metrics_space as ms on (true)
INNER JOIN metrics_space_compressed as msc on (true)
WHERE CASE WHEN m.device_id is not NULL and ms.v2 is not NULL THEN NULL::int2 end = msc.device_id;

