-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.


-- this should use DecompressChunk node
:PREFIX_VERBOSE SELECT * FROM :TEST_TABLE WHERE device_id = 1 ORDER BY time LIMIT 5;

-- test RECORD by itself
:PREFIX SELECT * FROM :TEST_TABLE WHERE device_id = 1 ORDER BY time;

-- test expressions
:PREFIX SELECT
  time_bucket('1d',time),
  v1 + v2 AS "sum",
  COALESCE(NULL,v1,v2) AS "coalesce",
  NULL AS "NULL",
  'text' AS "text",
  :TEST_TABLE AS "RECORD"
FROM :TEST_TABLE WHERE device_id IN (1,2) ORDER BY time, device_id;

-- test empty targetlist
:PREFIX SELECT FROM :TEST_TABLE;

-- test empty resultset
:PREFIX SELECT * FROM :TEST_TABLE WHERE device_id < 0;

-- test targetlist not referencing columns
:PREFIX SELECT 1 FROM :TEST_TABLE;

-- test constraints not present in targetlist
:PREFIX SELECT v1 FROM :TEST_TABLE WHERE device_id = 1 ORDER BY v1;

-- test order not present in targetlist
:PREFIX SELECT v2 FROM :TEST_TABLE WHERE device_id = 1 ORDER BY v1;

-- test column with all NULL
:PREFIX SELECT v3 FROM :TEST_TABLE WHERE device_id = 1;

--
-- test qual pushdown
--

-- v3 is not segment by or order by column so should not be pushed down
:PREFIX_VERBOSE SELECT * FROM :TEST_TABLE WHERE v3 > 10.0 ORDER BY time, device_id;

-- device_id constraint should be pushed down
:PREFIX SELECT * FROM :TEST_TABLE WHERE device_id = 1 ORDER BY time, device_id LIMIT 10;

-- test IS NULL / IS NOT NULL
:PREFIX SELECT * FROM :TEST_TABLE WHERE device_id IS NOT NULL ORDER BY time, device_id LIMIT 10;
:PREFIX SELECT * FROM :TEST_TABLE WHERE device_id IS NULL ORDER BY time, device_id LIMIT 10;

-- test IN (Const,Const)
:PREFIX SELECT * FROM :TEST_TABLE WHERE device_id IN (1,2) ORDER BY time, device_id LIMIT 10;

-- test cast pushdown
:PREFIX SELECT * FROM :TEST_TABLE WHERE device_id = '1'::text::int ORDER BY time, device_id LIMIT 10;

--test var op var with two segment by
:PREFIX SELECT * FROM :TEST_TABLE WHERE device_id = device_id_peer ORDER BY time, device_id LIMIT 10;
:PREFIX SELECT * FROM :TEST_TABLE WHERE device_id_peer < device_id ORDER BY time, device_id LIMIT 10;

-- test expressions
:PREFIX SELECT * FROM :TEST_TABLE WHERE device_id =  1 + 4/2 ORDER BY time, device_id LIMIT 10;

-- test function calls
-- not yet pushed down
:PREFIX SELECT * FROM :TEST_TABLE WHERE device_id = length(substring(version(),1,3)) ORDER BY time, device_id LIMIT 10;

--
-- test segment meta pushdown
--

-- order by column and const
:PREFIX SELECT * FROM :TEST_TABLE WHERE time = '2000-01-01 1:00:00+0' ORDER BY time, device_id LIMIT 10;
:PREFIX SELECT * FROM :TEST_TABLE WHERE time < '2000-01-01 1:00:00+0' ORDER BY time, device_id LIMIT 10;
:PREFIX SELECT * FROM :TEST_TABLE WHERE time <= '2000-01-01 1:00:00+0' ORDER BY time, device_id LIMIT 10;
:PREFIX SELECT * FROM :TEST_TABLE WHERE time >= '2000-01-01 1:00:00+0' ORDER BY time, device_id LIMIT 10;
:PREFIX SELECT * FROM :TEST_TABLE WHERE time > '2000-01-01 1:00:00+0' ORDER BY time, device_id LIMIT 10;
:PREFIX SELECT * FROM :TEST_TABLE WHERE '2000-01-01 1:00:00+0' < time ORDER BY time, device_id LIMIT 10;

--pushdowns between order by and segment by columns
:PREFIX SELECT * FROM :TEST_TABLE WHERE v0 < 1 ORDER BY time, device_id LIMIT 10;
:PREFIX SELECT * FROM :TEST_TABLE WHERE v0 < device_id ORDER BY time, device_id LIMIT 10;
:PREFIX SELECT * FROM :TEST_TABLE WHERE device_id < v0 ORDER BY time, device_id LIMIT 10;
:PREFIX SELECT * FROM :TEST_TABLE WHERE v1 = device_id ORDER BY time, device_id LIMIT 10;

--pushdown between two order by column (not pushed down)
:PREFIX SELECT * FROM :TEST_TABLE WHERE v0 = v1 ORDER BY time, device_id LIMIT 10;

--pushdown of quals on order by and segment by cols anded together
:PREFIX_VERBOSE SELECT * FROM :TEST_TABLE WHERE time > '2000-01-01 1:00:00+0' and device_id = 1 ORDER BY time, device_id LIMIT 10;

--pushdown of quals on order by and segment by cols or together (not pushed down)
:PREFIX SELECT * FROM :TEST_TABLE WHERE time > '2000-01-01 1:00:00+0' or device_id = 1 ORDER BY time, device_id LIMIT 10;

--functions not yet optimized
:PREFIX SELECT * FROM :TEST_TABLE WHERE time < now() ORDER BY time, device_id LIMIT 10;

-- test sort optimization interaction

:PREFIX SELECT time FROM :TEST_TABLE ORDER BY time DESC LIMIT 10;
:PREFIX SELECT time,device_id FROM :TEST_TABLE ORDER BY time DESC, device_id LIMIT 10;
:PREFIX SELECT time,device_id FROM :TEST_TABLE ORDER BY device_id, time DESC LIMIT 10;

--
-- test ordered path
--

-- should not produce ordered path
:PREFIX_VERBOSE SELECT * FROM :TEST_TABLE WHERE time > '2000-01-08' ORDER BY time, device_id;

-- should produce ordered path
:PREFIX_VERBOSE SELECT * FROM :TEST_TABLE WHERE time > '2000-01-08' ORDER BY device_id,device_id_peer,v0,v1 desc,time;

-- test order by columns not in targetlist
:PREFIX_VERBOSE SELECT device_id, device_id_peer FROM :TEST_TABLE WHERE time > '2000-01-08' ORDER BY device_id,device_id_peer,v0,v1 desc,time LIMIT 100;

-- test ordering only by segmentby columns
-- should produce ordered path and not have sequence number in targetlist of compressed scan
:PREFIX_VERBOSE SELECT device_id, device_id_peer FROM :TEST_TABLE WHERE time > '2000-01-08' ORDER BY device_id,device_id_peer LIMIT 100;

-- should produce ordered path
-- only referencing PREFIX_VERBOSE should work
:PREFIX_VERBOSE SELECT device_id,device_id_peer,v0 FROM :TEST_TABLE WHERE time > '2000-01-08' ORDER BY device_id,device_id_peer,v0;

-- should produce ordered path
-- only referencing PREFIX_VERBOSE should work
:PREFIX_VERBOSE SELECT device_id,device_id_peer,v0, v1 FROM :TEST_TABLE WHERE time > '2000-01-08' ORDER BY device_id,device_id_peer,v0, v1 desc;

-- should not produce ordered path
:PREFIX_VERBOSE SELECT * FROM :TEST_TABLE WHERE time > '2000-01-08' ORDER BY device_id,device_id_peer,v0,v1 desc,time,v3;

-- should produce ordered path
-- ASC/DESC for segmentby columns can be pushed down
:PREFIX_VERBOSE SELECT * FROM :TEST_TABLE WHERE time > '2000-01-08' ORDER BY device_id DESC,device_id_peer DESC,v0,v1 desc,time;

-- should not produce ordered path
:PREFIX_VERBOSE SELECT * FROM :TEST_TABLE WHERE time > '2000-01-08' ORDER BY device_id DESC,device_id_peer DESC,v0,v1,time;

--
-- test constraint exclusion
--

-- test plan time exclusion
-- first chunk should be excluded
:PREFIX SELECT * FROM :TEST_TABLE WHERE time > '2000-01-08' ORDER BY time, device_id;

-- test runtime exclusion
-- first chunk should be excluded
:PREFIX SELECT * FROM :TEST_TABLE WHERE time > '2000-01-08'::text::timestamptz ORDER BY time, device_id;

-- test aggregate
:PREFIX SELECT count(*) FROM :TEST_TABLE;

-- test aggregate with GROUP BY
:PREFIX SELECT count(*) FROM :TEST_TABLE GROUP BY device_id ORDER BY device_id;

-- test window functions with GROUP BY
:PREFIX SELECT sum(count(*)) OVER () FROM :TEST_TABLE GROUP BY device_id ORDER BY device_id;

-- test CTE
:PREFIX WITH
q AS (SELECT v1 FROM :TEST_TABLE ORDER BY time)
SELECT * FROM q ORDER BY v1;

-- test CTE join
:PREFIX WITH
q1 AS (SELECT time, v1 FROM :TEST_TABLE WHERE device_id=1 ORDER BY time),
q2 AS (SELECT time, v2 FROM :TEST_TABLE WHERE device_id=2 ORDER BY time)
SELECT * FROM q1 INNER JOIN q2 ON q1.time=q2.time ORDER BY q1.time;

-- test prepared statement
PREPARE prep AS SELECT count(time) FROM :TEST_TABLE WHERE device_id = 1;
:PREFIX EXECUTE prep;
EXECUTE prep;
EXECUTE prep;
EXECUTE prep;
EXECUTE prep;
EXECUTE prep;
EXECUTE prep;
DEALLOCATE prep;

--
-- test indexes
--

SET enable_seqscan TO false;

-- IndexScans should work
:PREFIX_VERBOSE SELECT time, device_id FROM :TEST_TABLE WHERE device_id = 1 ORDER BY device_id, time;

-- globs should not plan IndexOnlyScans
:PREFIX_VERBOSE SELECT * FROM :TEST_TABLE WHERE device_id = 1 ORDER BY device_id, time;

-- whole row reference should work
:PREFIX_VERBOSE SELECT test_table FROM :TEST_TABLE as test_table WHERE device_id = 1 ORDER BY device_id, time;

-- even when we select only a segmentby column, we still need count
:PREFIX_VERBOSE SELECT device_id FROM :TEST_TABLE WHERE device_id = 1 ORDER BY device_id;

:PREFIX_VERBOSE SELECT count(*) FROM :TEST_TABLE WHERE device_id = 1;

-- should be able to order using an index
CREATE INDEX tmp_idx ON :TEST_TABLE(device_id);

:PREFIX_VERBOSE SELECT device_id FROM :TEST_TABLE ORDER BY device_id;

DROP INDEX tmp_idx CASCADE;

--use the peer index
:PREFIX_VERBOSE SELECT * FROM :TEST_TABLE WHERE device_id_peer = 1 ORDER BY device_id_peer, time;

:PREFIX_VERBOSE SELECT device_id_peer FROM :TEST_TABLE WHERE device_id_peer = 1 ORDER BY device_id_peer;

:PREFIX_VERBOSE SELECT device_id_peer FROM :TEST_TABLE WHERE device_id_peer IN (VALUES (1));

:PREFIX_VERBOSE SELECT device_id_peer FROM :TEST_TABLE WHERE device_id_peer IN (VALUES (1), (2));

:PREFIX_VERBOSE SELECT device_id_peer FROM :TEST_TABLE WHERE device_id IN (VALUES (1));

:PREFIX_VERBOSE SELECT device_id_peer FROM :TEST_TABLE WHERE device_id IN (VALUES (1), (2));

-- force a BitmapHeapScan
SET enable_indexscan TO false;
:PREFIX_VERBOSE SELECT device_id_peer FROM :TEST_TABLE WHERE device_id IN (VALUES (1));

SET enable_indexscan TO false;
:PREFIX_VERBOSE SELECT device_id_peer FROM :TEST_TABLE WHERE device_id IN (VALUES (1), (2));

SET enable_indexscan TO true;
SET enable_seqscan TO true;

-- test view
CREATE OR REPLACE ViEW compressed_view AS SELECT time, device_id, v1, v2 FROM :TEST_TABLE;
:PREFIX SELECT * FROM compressed_view WHERE device_id = 1 ORDER BY time DESC LIMIT 10;
DROP VIEW compressed_view;

-- test INNER JOIN
:PREFIX SELECT * FROM :TEST_TABLE m1 INNER JOIN :TEST_TABLE m2 ON m1.time = m2.time AND m1.device_id=m2.device_id ORDER BY m1.time, m1.device_id LIMIT 10;
:PREFIX SELECT * FROM :TEST_TABLE m1 INNER JOIN :TEST_TABLE m2 ON m1.time = m2.time INNER JOIN :TEST_TABLE m3 ON m2.time = m3.time AND m1.device_id=m2.device_id AND m3.device_id = 3 ORDER BY m1.time, m1.device_id LIMIT 10;
:PREFIX SELECT * FROM :TEST_TABLE m1 INNER JOIN :TEST_TABLE m2 ON m1.time = m2.time AND m1.device_id=1 AND m2.device_id=2 ORDER BY m1.time, m1.device_id, m2.time, m2.device_id LIMIT 100;
:PREFIX SELECT * FROM metrics m1 INNER JOIN metrics_space m2 ON m1.time = m2.time AND m1.device_id=1 AND m2.device_id=2 ORDER BY m1.time, m1.device_id, m2.time, m2.device_id LIMIT 100;

-- test OUTER JOIN
:PREFIX SELECT * FROM :TEST_TABLE m1 LEFT OUTER JOIN :TEST_TABLE m2 ON m1.time = m2.time AND m1.device_id=m2.device_id ORDER BY m1.time, m1.device_id LIMIT 10;
:PREFIX SELECT * FROM :TEST_TABLE m1 LEFT OUTER JOIN :TEST_TABLE m2 ON m1.time = m2.time AND m1.device_id=1 AND m2.device_id=2 ORDER BY m1.time, m1.device_id, m2.time, m2.device_id LIMIT 100;
:PREFIX SELECT * FROM metrics m1 LEFT OUTER JOIN metrics_space m2 ON m1.time = m2.time AND m1.device_id=1 AND m2.device_id=2 ORDER BY m1.time, m1.device_id, m2.time, m2.device_id LIMIT 100;

-- test implicit self-join
:PREFIX SELECT * FROM :TEST_TABLE m1, :TEST_TABLE m2 WHERE m1.time = m2.time ORDER BY m1.time, m1.device_id, m2.time, m2.device_id LIMIT 20;

-- test self-join with sub-query
:PREFIX SELECT * FROM (SELECT * FROM :TEST_TABLE m1) m1 INNER JOIN (SELECT * FROM :TEST_TABLE m2) m2 ON m1.time = m2.time ORDER BY m1.time, m1.device_id, m2.device_id LIMIT 10;

:PREFIX SELECT * FROM generate_series('2000-01-01'::timestamptz,'2000-02-01'::timestamptz,'1d'::interval) g(time) INNER JOIN LATERAL(SELECT time FROM :TEST_TABLE m1 WHERE m1.time = g.time LIMIT 1) m1 ON true;

-- test prepared statement with params pushdown
PREPARE param_prep(int) AS SELECT * FROM generate_series('2000-01-01'::timestamptz,'2000-02-01'::timestamptz,'1d'::interval) g(time) INNER JOIN LATERAL(SELECT time FROM :TEST_TABLE m1 WHERE m1.time = g.time AND device_id = $1 LIMIT 1) m1 ON true;

:PREFIX EXECUTE param_prep(1);
:PREFIX EXECUTE param_prep(2);
EXECUTE param_prep(1);
EXECUTE param_prep(2);
EXECUTE param_prep(1);
EXECUTE param_prep(2);
EXECUTE param_prep(1);
DEALLOCATE param_prep;

-- test continuous aggs
SET client_min_messages TO error;
CREATE VIEW cagg_test WITH (timescaledb.continuous) AS SELECT time_bucket('1d',time) AS time, device_id, avg(v1) FROM :TEST_TABLE WHERE device_id=1 GROUP BY 1,2;
REFRESH MATERIALIZED VIEW cagg_test;
SELECT time FROM cagg_test ORDER BY time LIMIT 1;
DROP VIEW cagg_test CASCADE;
RESET client_min_messages;

