-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--we want to see how our logic excludes chunks
--and not how much work constraint_exclusion does
SET constraint_exclusion = 'off';

--test upper bounds
:PREFIX SELECT * FROM hyper WHERE time < 10 ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE time < 11 ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE time = 10 ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE 10 >= time ORDER BY value;

--test lower bounds
:PREFIX SELECT * FROM hyper WHERE time >= 10 and time < 20 ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE 10 < time and 20 >= time ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE time >= 9 and time < 20 ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE time > 9 and time < 20 ORDER BY value;

--test empty result
:PREFIX SELECT * FROM hyper WHERE time < 0;

--test expression evaluation
:PREFIX SELECT * FROM hyper WHERE time < (5*2)::smallint;

--test logic at INT64_MAX
:PREFIX SELECT * FROM hyper WHERE time = 9223372036854775807::bigint ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE time = 9223372036854775806::bigint ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE time >= 9223372036854775807::bigint ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE time > 9223372036854775807::bigint ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE time > 9223372036854775806::bigint ORDER BY value;

--cte
:PREFIX WITH cte AS(
  SELECT * FROM hyper WHERE time < 10
)
SELECT * FROM cte ORDER BY value;

--subquery
:PREFIX SELECT 0 = ANY (SELECT value FROM hyper WHERE time < 10);

--no space constraint
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 ORDER BY value;

--valid space constraint
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 and device_id = 'dev5' ORDER BY value;
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 and 'dev5' = device_id ORDER BY value;
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 and 'dev'||(2+3) = device_id ORDER BY value;

--only space constraint
:PREFIX SELECT * FROM hyper_w_space WHERE 'dev5' = device_id ORDER BY value;

--unhandled space constraint
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 and device_id > 'dev5' ORDER BY value;

--use of OR - does not filter chunks
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 AND (device_id = 'dev5' or device_id = 'dev6') ORDER BY value;

--cte
:PREFIX WITH cte AS(
   SELECT * FROM hyper_w_space WHERE time < 10 and device_id = 'dev5'
)
SELECT * FROM cte ORDER BY value;

--subquery
:PREFIX SELECT 0 = ANY (SELECT value FROM hyper_w_space WHERE time < 10 and device_id = 'dev5');

--view
:PREFIX SELECT * FROM hyper_w_space_view WHERE time < 10 and device_id = 'dev5' ORDER BY value;

--IN statement - simple
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 AND device_id IN ('dev5') ORDER BY value;

--IN statement - two chunks
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 AND device_id IN ('dev5','dev6') ORDER BY value;

--IN statement - one chunk
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 AND device_id IN ('dev4','dev5') ORDER BY value;

--NOT IN - does not filter chunks
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 AND device_id NOT IN ('dev5','dev6') ORDER BY value;

--IN statement with subquery - does not filter chunks
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 AND device_id IN (SELECT 'dev5'::text) ORDER BY value;

--ANY
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 AND device_id = ANY(ARRAY['dev5','dev6']) ORDER BY value;

--ANY with intersection
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 AND device_id = ANY(ARRAY['dev5','dev6']) AND device_id = ANY(ARRAY['dev6','dev7']) ORDER BY value;

--ANY without intersection shouldn't scan any chunks
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 AND device_id = ANY(ARRAY['dev5','dev6']) AND device_id = ANY(ARRAY['dev8','dev9']) ORDER BY value;

--ANY/IN/ALL only works for equals operator
:PREFIX SELECT * FROM hyper_w_space WHERE device_id < ANY(ARRAY['dev5','dev6']) ORDER BY value;

--ALL with equals and different values shouldn't scan any chunks
:PREFIX SELECT * FROM hyper_w_space WHERE device_id = ALL(ARRAY['dev5','dev6']) ORDER BY value;

--Multi AND
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 AND time < 100 ORDER BY value;

--Time dimension doesn't filter chunks when using IN/ANY with multiple arguments
:PREFIX SELECT * FROM hyper_w_space WHERE time < ANY(ARRAY[1,2]) ORDER BY value;

--Time dimension chunk filtering works for ANY with single argument
:PREFIX SELECT * FROM hyper_w_space WHERE time < ANY(ARRAY[1]) ORDER BY value;

--Time dimension chunk filtering works for ALL with single argument
:PREFIX SELECT * FROM hyper_w_space WHERE time < ALL(ARRAY[1]) ORDER BY value;

--Time dimension chunk filtering works for ALL with multiple arguments
:PREFIX SELECT * FROM hyper_w_space WHERE time < ALL(ARRAY[1,10,20,30]) ORDER BY value;

--AND intersection using IN and EQUALS
:PREFIX SELECT * FROM hyper_w_space WHERE device_id IN ('dev1','dev2') AND device_id = 'dev1' ORDER BY value;

--AND with no intersection using IN and EQUALS
:PREFIX SELECT * FROM hyper_w_space WHERE device_id IN ('dev1','dev2') AND device_id = 'dev3' ORDER BY value;

--timestamps
--these should work since they are immutable functions
:PREFIX SELECT * FROM hyper_ts WHERE time < 'Wed Dec 31 16:00:10 1969 PST'::timestamptz ORDER BY value;
:PREFIX SELECT * FROM hyper_ts WHERE time < to_timestamp(10) ORDER BY value;
:PREFIX SELECT * FROM hyper_ts WHERE time < 'Wed Dec 31 16:00:10 1969'::timestamp AT TIME ZONE 'PST' ORDER BY value;
:PREFIX SELECT * FROM hyper_ts WHERE time < to_timestamp(10) and device_id = 'dev1' ORDER BY value;

--these should not work since uses stable functions;
:PREFIX SELECT * FROM hyper_ts WHERE time < 'Wed Dec 31 16:00:10 1969'::timestamp ORDER BY value;
:PREFIX SELECT * FROM hyper_ts WHERE time < ('Wed Dec 31 16:00:10 1969'::timestamp::timestamptz) ORDER BY value;
:PREFIX SELECT * FROM hyper_ts WHERE NOW() < time ORDER BY value;

--joins
:PREFIX SELECT * FROM hyper_ts WHERE tag_id IN (SELECT id FROM tag WHERE tag.id=1) and time < to_timestamp(10) and device_id = 'dev1' ORDER BY value;
:PREFIX SELECT * FROM hyper_ts WHERE tag_id IN (SELECT id FROM tag WHERE tag.id=1) or (time < to_timestamp(10) and device_id = 'dev1') ORDER BY value;
:PREFIX SELECT * FROM hyper_ts WHERE tag_id IN (SELECT id FROM tag WHERE tag.name='tag1') and time < to_timestamp(10) and device_id = 'dev1' ORDER BY value;
:PREFIX SELECT * FROM hyper_ts JOIN tag on (hyper_ts.tag_id = tag.id ) WHERE time < to_timestamp(10) and device_id = 'dev1' ORDER BY value;
:PREFIX SELECT * FROM hyper_ts JOIN tag on (hyper_ts.tag_id = tag.id ) WHERE tag.name = 'tag1' and time < to_timestamp(10) and device_id = 'dev1' ORDER BY value;

-- test constraint exclusion for constraints in ON clause of JOINs

-- should exclude chunks on m1 and propagate qual to m2 because of INNER JOIN
:PREFIX SELECT m1.time,m2.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz m2 ON m1.time = m2.time AND m1.time < '2000-01-10' ORDER BY m1.time;
-- should exclude chunks on m2 and propagate qual to m1 because of INNER JOIN
:PREFIX SELECT m1.time,m2.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz m2 ON m1.time = m2.time AND m2.time < '2000-01-10' ORDER BY m1.time;
-- must not exclude on m1
:PREFIX SELECT m1.time,m2.time FROM metrics_timestamptz m1 LEFT JOIN metrics_timestamptz m2 ON m1.time = m2.time AND m1.time < '2000-01-10' ORDER BY m1.time;
-- should exclude chunks on m2
:PREFIX SELECT m1.time,m2.time FROM metrics_timestamptz m1 LEFT JOIN metrics_timestamptz m2 ON m1.time = m2.time AND m2.time < '2000-01-10' ORDER BY m1.time;
-- should exclude chunks on m1
:PREFIX SELECT m1.time,m2.time FROM metrics_timestamptz m1 RIGHT JOIN metrics_timestamptz m2 ON m1.time = m2.time AND m1.time < '2000-01-10' ORDER BY m1.time;
-- must not exclude chunks on m2
:PREFIX SELECT m1.time,m2.time FROM metrics_timestamptz m1 RIGHT JOIN metrics_timestamptz m2 ON m1.time = m2.time AND m2.time < '2000-01-10' ORDER BY m1.time;

-- time_bucket exclusion
:PREFIX SELECT * FROM hyper WHERE time_bucket(10, time) < 10::bigint ORDER BY time;
:PREFIX SELECT * FROM hyper WHERE time_bucket(10, time) < 11::bigint ORDER BY time;
:PREFIX SELECT * FROM hyper WHERE time_bucket(10, time) <= 10::bigint ORDER BY time;
:PREFIX SELECT * FROM hyper WHERE 10::bigint > time_bucket(10, time) ORDER BY time;
:PREFIX SELECT * FROM hyper WHERE 11::bigint > time_bucket(10, time) ORDER BY time;

-- test overflow behaviour of time_bucket exclusion
:PREFIX SELECT * FROM hyper WHERE time > 950 AND time_bucket(10, time) < '9223372036854775807'::bigint ORDER BY time;

-- test timestamp upper boundary
-- this will error because even though the transformation results in a valid timestamp
-- our supported range of values for time is smaller then postgres
\set ON_ERROR_STOP 0
:PREFIX SELECT * FROM metrics_timestamp WHERE time_bucket('1d',time) < '294276-01-01'::timestamp ORDER BY time;
\set ON_ERROR_STOP 1
-- transformation would be out of range
:PREFIX SELECT * FROM metrics_timestamp WHERE time_bucket('1000d',time) < '294276-01-01'::timestamp ORDER BY time;

-- test timestamptz upper boundary
-- this will error because even though the transformation results in a valid timestamp
-- our supported range of values for time is smaller then postgres
\set ON_ERROR_STOP 0
:PREFIX SELECT time FROM metrics_timestamptz WHERE time_bucket('1d',time) < '294276-01-01'::timestamptz ORDER BY time;
\set ON_ERROR_STOP 1
-- transformation would be out of range
:PREFIX SELECT time FROM metrics_timestamptz WHERE time_bucket('1000d',time) < '294276-01-01'::timestamptz ORDER BY time;

:PREFIX SELECT * FROM hyper WHERE time_bucket(10, time) > 10 AND time_bucket(10, time) < 100 ORDER BY time;
:PREFIX SELECT * FROM hyper WHERE time_bucket(10, time) > 10 AND time_bucket(10, time) < 20 ORDER BY time;
:PREFIX SELECT * FROM hyper WHERE time_bucket(1, time) > 11 AND time_bucket(1, time) < 19 ORDER BY time;
:PREFIX SELECT * FROM hyper WHERE 10 < time_bucket(10, time) AND 20 > time_bucket(10,time) ORDER BY time;

-- time_bucket exclusion with date
:PREFIX SELECT * FROM metrics_date WHERE time_bucket('1d',time) < '2000-01-03' ORDER BY time;
:PREFIX SELECT * FROM metrics_date WHERE time_bucket('1d',time) >= '2000-01-03' AND time_bucket('1d',time) <= '2000-01-10' ORDER BY time;

-- time_bucket exclusion with timestamp
:PREFIX SELECT * FROM metrics_timestamp WHERE time_bucket('1d',time) < '2000-01-03' ORDER BY time;
:PREFIX SELECT * FROM metrics_timestamp WHERE time_bucket('1d',time) >= '2000-01-03' AND time_bucket('1d',time) <= '2000-01-10' ORDER BY time;

-- time_bucket exclusion with timestamptz
:PREFIX SELECT time FROM metrics_timestamptz WHERE time_bucket('6h',time) < '2000-01-03' ORDER BY time;
:PREFIX SELECT time FROM metrics_timestamptz WHERE time_bucket('6h',time) >= '2000-01-03' AND time_bucket('6h',time) <= '2000-01-10' ORDER BY time;

-- time_bucket exclusion with timestamptz and day interval
:PREFIX SELECT time FROM metrics_timestamptz WHERE time_bucket('1d',time) < '2000-01-03' ORDER BY time;
:PREFIX SELECT time FROM metrics_timestamptz WHERE time_bucket('1d',time) >= '2000-01-03' AND time_bucket('1d',time) <= '2000-01-10' ORDER BY time;
:PREFIX SELECT time FROM metrics_timestamptz WHERE time_bucket('1d',time) >= '2000-01-03' AND time_bucket('7d',time) <= '2000-01-10' ORDER BY time;

--exclude chunks based on time column with partitioning function. This
--transparently applies the time partitioning function on the time
--value to be able to exclude chunks (similar to a closed dimension).
:PREFIX SELECT * FROM hyper_timefunc WHERE time < 4 ORDER BY value;
--excluding based on time expression is currently unoptimized
:PREFIX SELECT * FROM hyper_timefunc WHERE unix_to_timestamp(time) < 'Wed Dec 31 16:00:04 1969 PST' ORDER BY value;

-- test qual propagation for joins
RESET constraint_exclusion;

-- nothing to propagate
:PREFIX SELECT m1.time FROM metrics_timestamptz m1, metrics_timestamptz m2 WHERE m1.time = m2.time ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz m2 ON m1.time = m2.time ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 LEFT JOIN metrics_timestamptz m2 ON m1.time = m2.time ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 RIGHT JOIN metrics_timestamptz m2 ON m1.time = m2.time ORDER BY m1.time;

-- OR constraints should not propagate
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz m2 ON m1.time = m2.time WHERE m1.time < '2000-01-10' OR m1.time > '2001-01-01' ORDER BY m1.time;

-- test single constraint
-- constraint should be on both scans
-- these will propagate even for LEFT/RIGHT JOIN because the constraints are not in the ON clause and therefore imply a NOT NULL condition on the JOIN column
:PREFIX SELECT m1.time FROM metrics_timestamptz m1, metrics_timestamptz m2 WHERE m1.time = m2.time AND m1.time < '2000-01-10' ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz m2 ON m1.time = m2.time WHERE m1.time < '2000-01-10' ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 LEFT JOIN metrics_timestamptz m2 ON m1.time = m2.time WHERE m1.time < '2000-01-10' ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 RIGHT JOIN metrics_timestamptz m2 ON m1.time = m2.time WHERE m1.time < '2000-01-10' ORDER BY m1.time;

-- test 2 constraints on single relation
-- these will propagate even for LEFT/RIGHT JOIN because the constraints are not in the ON clause and therefore imply a NOT NULL condition on the JOIN column
:PREFIX SELECT m1.time FROM metrics_timestamptz m1, metrics_timestamptz m2 WHERE m1.time = m2.time AND m1.time > '2000-01-01' AND m1.time < '2000-01-10' ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz m2 ON m1.time = m2.time WHERE m1.time > '2000-01-01' AND m1.time < '2000-01-10' ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 LEFT JOIN metrics_timestamptz m2 ON m1.time = m2.time WHERE m1.time > '2000-01-01' AND m1.time < '2000-01-10' ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 RIGHT JOIN metrics_timestamptz m2 ON m1.time = m2.time WHERE m1.time > '2000-01-01' AND m1.time < '2000-01-10' ORDER BY m1.time;

-- test 2 constraints with 1 constraint on each relation
-- these will propagate even for LEFT/RIGHT JOIN because the constraints are not in the ON clause and therefore imply a NOT NULL condition on the JOIN column
:PREFIX SELECT m1.time FROM metrics_timestamptz m1, metrics_timestamptz m2 WHERE m1.time = m2.time AND m1.time > '2000-01-01' AND m2.time < '2000-01-10' ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz m2 ON m1.time = m2.time WHERE m1.time > '2000-01-01' AND m2.time < '2000-01-10' ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 LEFT JOIN metrics_timestamptz m2 ON m1.time = m2.time WHERE m1.time > '2000-01-01' AND m2.time < '2000-01-10' ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 RIGHT JOIN metrics_timestamptz m2 ON m1.time = m2.time WHERE m1.time > '2000-01-01' AND m2.time < '2000-01-10' ORDER BY m1.time;

-- test constraints in ON clause of INNER JOIN
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz m2 ON m1.time = m2.time AND m2.time > '2000-01-01' AND m2.time < '2000-01-10' ORDER BY m1.time;

-- test constraints in ON clause of LEFT JOIN
-- must not propagate
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 LEFT JOIN metrics_timestamptz m2 ON m1.time = m2.time AND m2.time > '2000-01-01' AND m2.time < '2000-01-10' ORDER BY m1.time;

-- test constraints in ON clause of RIGHT JOIN
-- must not propagate
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 RIGHT JOIN metrics_timestamptz m2 ON m1.time = m2.time AND m2.time > '2000-01-01' AND m2.time < '2000-01-10' ORDER BY m1.time;

-- test equality condition not in ON clause
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz m2 ON true WHERE m2.time = m1.time AND m2.time < '2000-01-10' ORDER BY m1.time;

-- test constraints not joined on
-- device_id constraint must not propagate
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz m2 ON true WHERE m2.time = m1.time AND m2.time < '2000-01-10' AND m1.device_id = 1 ORDER BY m1.time;

-- test multiple join conditions
-- device_id constraint should propagate
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz m2 ON true WHERE m2.time = m1.time AND m1.device_id = m2.device_id AND m2.time < '2000-01-10' AND m1.device_id = 1 ORDER BY m1.time;

-- test join with 3 tables
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz m2 ON m1.time = m2.time INNER JOIN metrics_timestamptz m3 ON m2.time=m3.time WHERE m1.time > '2000-01-01' AND m1.time < '2000-01-10' ORDER BY m1.time;

-- test non-Const constraints
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz m2 ON m1.time = m2.time WHERE m1.time < '2000-01-10'::text::timestamptz ORDER BY m1.time;

-- test now()
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz m2 ON m1.time = m2.time WHERE m1.time < now() ORDER BY m1.time;

-- test volatile function
-- should not propagate
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz m2 ON m1.time = m2.time WHERE m1.time < clock_timestamp() ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz m2 ON m1.time = m2.time WHERE m2.time < clock_timestamp() ORDER BY m1.time;

-- test JOINs with normal table
-- will not propagate because constraints are only added to hypertables
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN regular_timestamptz m2 ON m1.time = m2.time WHERE m1.time < '2000-01-10' ORDER BY m1.time;

-- test JOINs with normal table
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN regular_timestamptz m2 ON m1.time = m2.time WHERE m2.time < '2000-01-10' ORDER BY m1.time;

