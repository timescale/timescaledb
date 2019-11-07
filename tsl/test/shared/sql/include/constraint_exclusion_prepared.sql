
-- test prepared statements
-- executor startup exclusion with no chunks excluded
PREPARE prep AS SELECT time FROM :TEST_TABLE WHERE time < now() AND device_id = 1 ORDER BY time;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
DEALLOCATE prep;

-- executor startup exclusion with chunks excluded
PREPARE prep AS SELECT time FROM :TEST_TABLE WHERE time < '2000-01-10'::text::timestamptz AND device_id = 1 ORDER BY time;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
DEALLOCATE prep;

-- runtime exclusion with LATERAL and 2 hypertables
PREPARE prep AS SELECT m1.time, m2.time FROM :TEST_TABLE m1 LEFT JOIN LATERAL(SELECT time FROM :TEST_TABLE m2 WHERE m1.time = m2.time LIMIT 1) m2 ON true WHERE device_id = 2 ORDER BY m1.time;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
DEALLOCATE prep;

-- executor startup exclusion with subquery
PREPARE prep AS SELECT time FROM (SELECT time FROM :TEST_TABLE WHERE time < '2000-01-10'::text::timestamptz ORDER BY time LIMIT 100) m;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
DEALLOCATE prep;

-- test constraint exclusion for subqueries with ConstraintAwareAppend
SET timescaledb.enable_chunk_append TO false;
PREPARE prep AS SELECT device_id, time FROM (SELECT device_id, time FROM :TEST_TABLE WHERE time < '2000-01-10'::text::timestamptz ORDER BY device_id, time LIMIT 100) m;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
:PREFIX EXECUTE prep;
DEALLOCATE prep;
RESET timescaledb.enable_chunk_append;

