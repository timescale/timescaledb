-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--we want to see how our logic excludes chunks
--and not how much work constraint_exclusion does
SET constraint_exclusion = 'off';

\qecho test upper bounds
:PREFIX SELECT * FROM hyper WHERE time < 10 ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE time < 11 ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE time = 10 ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE 10 >= time ORDER BY value;

\qecho test lower bounds
:PREFIX SELECT * FROM hyper WHERE time >= 10 and time < 20 ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE 10 < time and 20 >= time ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE time >= 9 and time < 20 ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE time > 9 and time < 20 ORDER BY value;

\qecho test empty result
:PREFIX SELECT * FROM hyper WHERE time < 0;

\qecho test expression evaluation
:PREFIX SELECT * FROM hyper WHERE time < (5*2)::smallint;

\qecho test logic at INT64_MAX
:PREFIX SELECT * FROM hyper WHERE time = 9223372036854775807::bigint ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE time = 9223372036854775806::bigint ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE time >= 9223372036854775807::bigint ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE time > 9223372036854775807::bigint ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE time > 9223372036854775806::bigint ORDER BY value;

\qecho cte
:PREFIX WITH cte AS(
  SELECT * FROM hyper WHERE time < 10
)
SELECT * FROM cte ORDER BY value;

\qecho subquery
:PREFIX SELECT 0 = ANY (SELECT value FROM hyper WHERE time < 10);

\qecho no space constraint
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 ORDER BY value;

\qecho valid space constraint
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 and device_id = 'dev5' ORDER BY value;
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 and 'dev5' = device_id ORDER BY value;
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 and 'dev'||(2+3) = device_id ORDER BY value;

\qecho only space constraint
:PREFIX SELECT * FROM hyper_w_space WHERE 'dev5' = device_id ORDER BY value;

\qecho unhandled space constraint
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 and device_id > 'dev5' ORDER BY value;

\qecho use of OR - does not filter chunks
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 AND (device_id = 'dev5' or device_id = 'dev6') ORDER BY value;

\qecho cte
:PREFIX WITH cte AS(
   SELECT * FROM hyper_w_space WHERE time < 10 and device_id = 'dev5'
)
SELECT * FROM cte ORDER BY value;

\qecho subquery
:PREFIX SELECT 0 = ANY (SELECT value FROM hyper_w_space WHERE time < 10 and device_id = 'dev5');

\qecho view
:PREFIX SELECT * FROM hyper_w_space_view WHERE time < 10 and device_id = 'dev5' ORDER BY value;

\qecho IN statement - simple
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 AND device_id IN ('dev5') ORDER BY value;

\qecho IN statement - two chunks
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 AND device_id IN ('dev5','dev6') ORDER BY value;

\qecho IN statement - one chunk
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 AND device_id IN ('dev4','dev5') ORDER BY value;

\qecho NOT IN - does not filter chunks
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 AND device_id NOT IN ('dev5','dev6') ORDER BY value;

\qecho IN statement with subquery - does not filter chunks
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 AND device_id IN (SELECT 'dev5'::text) ORDER BY value;

\qecho ANY
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 AND device_id = ANY(ARRAY['dev5','dev6']) ORDER BY value;

\qecho ANY with intersection
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 AND device_id = ANY(ARRAY['dev5','dev6']) AND device_id = ANY(ARRAY['dev6','dev7']) ORDER BY value;

\qecho ANY without intersection shouldnt scan any chunks
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 AND device_id = ANY(ARRAY['dev5','dev6']) AND device_id = ANY(ARRAY['dev8','dev9']) ORDER BY value;

\qecho ANY/IN/ALL only works for equals operator
:PREFIX SELECT * FROM hyper_w_space WHERE device_id < ANY(ARRAY['dev5','dev6']) ORDER BY value;

\qecho ALL with equals and different values shouldnt scan any chunks
:PREFIX SELECT * FROM hyper_w_space WHERE device_id = ALL(ARRAY['dev5','dev6']) ORDER BY value;

\qecho Multi AND
:PREFIX SELECT * FROM hyper_w_space WHERE time < 10 AND time < 100 ORDER BY value;

\qecho Time dimension doesnt filter chunks when using IN/ANY with multiple arguments
:PREFIX SELECT * FROM hyper_w_space WHERE time < ANY(ARRAY[1,2]) ORDER BY value;

\qecho Time dimension chunk filtering works for ANY with single argument
:PREFIX SELECT * FROM hyper_w_space WHERE time < ANY(ARRAY[1]) ORDER BY value;

\qecho Time dimension chunk filtering works for ALL with single argument
:PREFIX SELECT * FROM hyper_w_space WHERE time < ALL(ARRAY[1]) ORDER BY value;

\qecho Time dimension chunk filtering works for ALL with multiple arguments
:PREFIX SELECT * FROM hyper_w_space WHERE time < ALL(ARRAY[1,10,20,30]) ORDER BY value;

\qecho AND intersection using IN and EQUALS
:PREFIX SELECT * FROM hyper_w_space WHERE device_id IN ('dev1','dev2') AND device_id = 'dev1' ORDER BY value;

\qecho AND with no intersection using IN and EQUALS
:PREFIX SELECT * FROM hyper_w_space WHERE device_id IN ('dev1','dev2') AND device_id = 'dev3' ORDER BY value;

\qecho timestamps
\qecho these should work since they are immutable functions
:PREFIX SELECT * FROM hyper_ts WHERE time < 'Wed Dec 31 16:00:10 1969 PST'::timestamptz ORDER BY value;
:PREFIX SELECT * FROM hyper_ts WHERE time < to_timestamp(10) ORDER BY value;
:PREFIX SELECT * FROM hyper_ts WHERE time < 'Wed Dec 31 16:00:10 1969'::timestamp AT TIME ZONE 'PST' ORDER BY value;
:PREFIX SELECT * FROM hyper_ts WHERE time < to_timestamp(10) and device_id = 'dev1' ORDER BY value;

\qecho these should not work since uses stable functions;
:PREFIX SELECT * FROM hyper_ts WHERE time < 'Wed Dec 31 16:00:10 1969'::timestamp ORDER BY value;
:PREFIX SELECT * FROM hyper_ts WHERE time < ('Wed Dec 31 16:00:10 1969'::timestamp::timestamptz) ORDER BY value;
:PREFIX SELECT * FROM hyper_ts WHERE NOW() < time ORDER BY value;

\qecho joins
:PREFIX SELECT * FROM hyper_ts WHERE tag_id IN (SELECT id FROM tag WHERE tag.id=1) and time < to_timestamp(10) and device_id = 'dev1' ORDER BY value;
:PREFIX SELECT * FROM hyper_ts WHERE tag_id IN (SELECT id FROM tag WHERE tag.id=1) or (time < to_timestamp(10) and device_id = 'dev1') ORDER BY value;
:PREFIX SELECT * FROM hyper_ts WHERE tag_id IN (SELECT id FROM tag WHERE tag.name='tag1') and time < to_timestamp(10) and device_id = 'dev1' ORDER BY value;
:PREFIX SELECT * FROM hyper_ts JOIN tag on (hyper_ts.tag_id = tag.id ) WHERE time < to_timestamp(10) and device_id = 'dev1' ORDER BY value;
:PREFIX SELECT * FROM hyper_ts JOIN tag on (hyper_ts.tag_id = tag.id ) WHERE tag.name = 'tag1' and time < to_timestamp(10) and device_id = 'dev1' ORDER BY value;

\qecho test constraint exclusion for constraints in ON clause of JOINs
\qecho should exclude chunks on m1 and propagate qual to m2 because of INNER JOIN
:PREFIX SELECT m1.time,m2.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time AND m1.time < '2000-01-10' ORDER BY m1.time;

\qecho should exclude chunks on m2 and propagate qual to m1 because of INNER JOIN
:PREFIX SELECT m1.time,m2.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time AND m2.time < '2000-01-10' ORDER BY m1.time;

\qecho must not exclude on m1
:PREFIX SELECT m1.time,m2.time FROM metrics_timestamptz m1 LEFT JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time AND m1.time < '2000-01-10' ORDER BY m1.time;

\qecho should exclude chunks on m2
:PREFIX SELECT m1.time,m2.time FROM metrics_timestamptz m1 LEFT JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time AND m2.time < '2000-01-10' ORDER BY m1.time;

\qecho should exclude chunks on m1
:PREFIX SELECT m1.time,m2.time FROM metrics_timestamptz m1 RIGHT JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time AND m1.time < '2000-01-10' ORDER BY m1.time;

\qecho must not exclude chunks on m2
:PREFIX SELECT m1.time,m2.time FROM metrics_timestamptz m1 RIGHT JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time AND m2.time < '2000-01-10' ORDER BY m1.time;

\qecho time_bucket exclusion
:PREFIX SELECT * FROM hyper WHERE time_bucket(10, time) < 10::bigint ORDER BY time;
:PREFIX SELECT * FROM hyper WHERE time_bucket(10, time) < 11::bigint ORDER BY time;
:PREFIX SELECT * FROM hyper WHERE time_bucket(10, time) <= 10::bigint ORDER BY time;
:PREFIX SELECT * FROM hyper WHERE 10::bigint > time_bucket(10, time) ORDER BY time;
:PREFIX SELECT * FROM hyper WHERE 11::bigint > time_bucket(10, time) ORDER BY time;

\qecho test overflow behaviour of time_bucket exclusion
:PREFIX SELECT * FROM hyper WHERE time > 950 AND time_bucket(10, time) < '9223372036854775807'::bigint ORDER BY time;

\qecho test timestamp upper boundary
\qecho there should be no transformation if we are out of the supported (TimescaleDB-specific) range
:PREFIX SELECT * FROM metrics_timestamp WHERE time_bucket('1d',time) < '294276-01-01'::timestamp ORDER BY time;
\qecho transformation would be out of range
:PREFIX SELECT * FROM metrics_timestamp WHERE time_bucket('1000d',time) < '294276-01-01'::timestamp ORDER BY time;

\qecho test timestamptz upper boundary
\qecho there should be no transformation if we are out of the supported (TimescaleDB-specific) range
:PREFIX SELECT time FROM metrics_timestamptz WHERE time_bucket('1d',time) < '294276-01-01'::timestamptz ORDER BY time;
\qecho transformation would be out of range
:PREFIX SELECT time FROM metrics_timestamptz WHERE time_bucket('1000d',time) < '294276-01-01'::timestamptz ORDER BY time;

:PREFIX SELECT * FROM hyper WHERE time_bucket(10, time) > 10 AND time_bucket(10, time) < 100 ORDER BY time;
:PREFIX SELECT * FROM hyper WHERE time_bucket(10, time) > 10 AND time_bucket(10, time) < 20 ORDER BY time;
:PREFIX SELECT * FROM hyper WHERE time_bucket(1, time) > 11 AND time_bucket(1, time) < 19 ORDER BY time;
:PREFIX SELECT * FROM hyper WHERE 10 < time_bucket(10, time) AND 20 > time_bucket(10,time) ORDER BY time;

\qecho time_bucket exclusion with date
:PREFIX SELECT * FROM metrics_date WHERE time_bucket('1d',time) < '2000-01-03' ORDER BY time;
:PREFIX SELECT * FROM metrics_date WHERE time_bucket('1d',time) >= '2000-01-03' AND time_bucket('1d',time) <= '2000-01-10' ORDER BY time;

\qecho time_bucket exclusion with timestamp
:PREFIX SELECT * FROM metrics_timestamp WHERE time_bucket('1d',time) < '2000-01-03' ORDER BY time;
:PREFIX SELECT * FROM metrics_timestamp WHERE time_bucket('1d',time) >= '2000-01-03' AND time_bucket('1d',time) <= '2000-01-10' ORDER BY time;

\qecho time_bucket exclusion with timestamptz
:PREFIX SELECT time FROM metrics_timestamptz WHERE time_bucket('6h',time) < '2000-01-03' ORDER BY time;
:PREFIX SELECT time FROM metrics_timestamptz WHERE time_bucket('6h',time) >= '2000-01-03' AND time_bucket('6h',time) <= '2000-01-10' ORDER BY time;

\qecho time_bucket exclusion with timestamptz and day interval
:PREFIX SELECT time FROM metrics_timestamptz WHERE time_bucket('1d',time) < '2000-01-03' ORDER BY time;
:PREFIX SELECT time FROM metrics_timestamptz WHERE time_bucket('1d',time) >= '2000-01-03' AND time_bucket('1d',time) <= '2000-01-10' ORDER BY time;
:PREFIX SELECT time FROM metrics_timestamptz WHERE time_bucket('1d',time) >= '2000-01-03' AND time_bucket('7d',time) <= '2000-01-10' ORDER BY time;

\qecho no transformation
:PREFIX SELECT * FROM hyper WHERE time_bucket(10 + floor(random())::int, time) > 10 AND time_bucket(10 + floor(random())::int, time) < 100 AND time < 150 ORDER BY time;

\qecho exclude chunks based on time column with partitioning function. This
\qecho transparently applies the time partitioning function on the time
\qecho value to be able to exclude chunks (similar to a closed dimension).
:PREFIX SELECT * FROM hyper_timefunc WHERE time < 4 ORDER BY value;
\qecho excluding based on time expression is currently unoptimized
:PREFIX SELECT * FROM hyper_timefunc WHERE unix_to_timestamp(time) < 'Wed Dec 31 16:00:04 1969 PST' ORDER BY value;

\qecho test qual propagation for joins
RESET constraint_exclusion;

\qecho nothing to propagate
:PREFIX SELECT m1.time FROM metrics_timestamptz m1, metrics_timestamptz_2 m2 WHERE m1.time = m2.time ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 LEFT JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 RIGHT JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time ORDER BY m1.time;

\qecho OR constraints should not propagate
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time WHERE m1.time < '2000-01-10' OR m1.time > '2001-01-01' ORDER BY m1.time;

\qecho test single constraint
\qecho constraint should be on both scans
\qecho these will propagate even for LEFT/RIGHT JOIN because the constraints are not in the ON clause and therefore imply a NOT NULL condition on the JOIN column
:PREFIX SELECT m1.time FROM metrics_timestamptz m1, metrics_timestamptz_2 m2 WHERE m1.time = m2.time AND m1.time < '2000-01-10' ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time WHERE m1.time < '2000-01-10' ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 LEFT JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time WHERE m1.time < '2000-01-10' ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 RIGHT JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time WHERE m1.time < '2000-01-10' ORDER BY m1.time;

\qecho test 2 constraints on single relation
\qecho these will propagate even for LEFT/RIGHT JOIN because the constraints are not in the ON clause and therefore imply a NOT NULL condition on the JOIN column
:PREFIX SELECT m1.time FROM metrics_timestamptz m1, metrics_timestamptz_2 m2 WHERE m1.time = m2.time AND m1.time > '2000-01-01' AND m1.time < '2000-01-10' ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time WHERE m1.time > '2000-01-01' AND m1.time < '2000-01-10' ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 LEFT JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time WHERE m1.time > '2000-01-01' AND m1.time < '2000-01-10' ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 RIGHT JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time WHERE m1.time > '2000-01-01' AND m1.time < '2000-01-10' ORDER BY m1.time;

\qecho test 2 constraints with 1 constraint on each relation
\qecho these will propagate even for LEFT/RIGHT JOIN because the constraints are not in the ON clause and therefore imply a NOT NULL condition on the JOIN column
:PREFIX SELECT m1.time FROM metrics_timestamptz m1, metrics_timestamptz_2 m2 WHERE m1.time = m2.time AND m1.time > '2000-01-01' AND m2.time < '2000-01-10' ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time WHERE m1.time > '2000-01-01' AND m2.time < '2000-01-10' ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 LEFT JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time WHERE m1.time > '2000-01-01' AND m2.time < '2000-01-10' ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 RIGHT JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time WHERE m1.time > '2000-01-01' AND m2.time < '2000-01-10' ORDER BY m1.time;

\qecho test constraints in ON clause of INNER JOIN
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time AND m2.time > '2000-01-01' AND m2.time < '2000-01-10' ORDER BY m1.time;

\qecho test constraints in ON clause of LEFT JOIN
\qecho must not propagate
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 LEFT JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time AND m2.time > '2000-01-01' AND m2.time < '2000-01-10' ORDER BY m1.time;

\qecho test constraints in ON clause of RIGHT JOIN
\qecho must not propagate
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 RIGHT JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time AND m2.time > '2000-01-01' AND m2.time < '2000-01-10' ORDER BY m1.time;

\qecho test equality condition not in ON clause
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz_2 m2 ON true WHERE m2.time = m1.time AND m2.time < '2000-01-10' ORDER BY m1.time;

\qecho test constraints not joined on
\qecho device_id constraint must not propagate
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz_2 m2 ON true WHERE m2.time = m1.time AND m2.time < '2000-01-10' AND m1.device_id = 1 ORDER BY m1.time;

\qecho test multiple join conditions
\qecho device_id constraint should propagate
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz_2 m2 ON true WHERE m2.time = m1.time AND m1.device_id = m2.device_id AND m2.time < '2000-01-10' AND m1.device_id = 1 ORDER BY m1.time;

\qecho test join with 3 tables
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time INNER JOIN metrics_timestamptz m3 ON m2.time=m3.time WHERE m1.time > '2000-01-01' AND m1.time < '2000-01-10' ORDER BY m1.time;

\qecho test non-Const constraints
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time WHERE m1.time < '2000-01-10'::text::timestamptz ORDER BY m1.time;

\qecho test now()
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time WHERE m1.time < now() ORDER BY m1.time;

\qecho test volatile function
\qecho should not propagate
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time WHERE m1.time < clock_timestamp() ORDER BY m1.time;
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN metrics_timestamptz_2 m2 ON m1.time = m2.time WHERE m2.time < clock_timestamp() ORDER BY m1.time;

\qecho test JOINs with normal table
\qecho will not propagate because constraints are only added to hypertables
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN regular_timestamptz m2 ON m1.time = m2.time WHERE m1.time < '2000-01-10' ORDER BY m1.time;

\qecho test JOINs with normal table
:PREFIX SELECT m1.time FROM metrics_timestamptz m1 INNER JOIN regular_timestamptz m2 ON m1.time = m2.time WHERE m2.time < '2000-01-10' ORDER BY m1.time;

\qecho test quals are not pushed into OUTER JOIN
CREATE TABLE outer_join_1 (id int, name text,time timestamptz NOT NULL DEFAULT '2000-01-01');
CREATE TABLE outer_join_2 (id int, name text,time timestamptz NOT NULL DEFAULT '2000-01-01');

SELECT (SELECT table_name FROM create_hypertable(tbl, 'time')) FROM (VALUES ('outer_join_1'),('outer_join_2')) v(tbl);

INSERT INTO outer_join_1 VALUES(1,'a'), (2,'b');
INSERT INTO outer_join_2 VALUES(1,'a');

:PREFIX SELECT one.id, two.name FROM outer_join_1 one LEFT OUTER JOIN outer_join_2 two ON one.id=two.id WHERE one.id=2;
:PREFIX SELECT one.id, two.name FROM outer_join_2 two RIGHT OUTER JOIN outer_join_1 one ON one.id=two.id WHERE one.id=2;

DROP TABLE outer_join_1;
DROP TABLE outer_join_2;

-- test UNION between regular table and hypertable
SELECT time FROM regular_timestamptz UNION SELECT time FROM metrics_timestamptz ORDER BY 1;

-- test UNION ALL between regular table and hypertable
SELECT time FROM regular_timestamptz UNION ALL SELECT time FROM metrics_timestamptz ORDER BY 1;

-- test nested join qual propagation
:PREFIX
SELECT * FROM (
SELECT o1_m1.time FROM metrics_timestamptz o1_m1 INNER JOIN metrics_timestamptz_2 o1_m2 ON true WHERE o1_m2.time = o1_m1.time AND o1_m1.device_id = o1_m2.device_id AND o1_m2.time < '2000-01-10' AND o1_m1.device_id = 1
) o1 FULL OUTER JOIN (
SELECT o2_m1.time FROM metrics_timestamptz o2_m1 FULL OUTER JOIN metrics_timestamptz_2 o2_m2 ON true WHERE o2_m2.time = o2_m1.time AND o2_m1.device_id = o2_m2.device_id AND o2_m2.time > '2000-01-20' AND o2_m1.device_id = 2
) o2 ON o1.time = o2.time ORDER BY 1,2;

:PREFIX
SELECT * FROM (
SELECT o1_m1.time FROM metrics_timestamptz o1_m1 INNER JOIN metrics_timestamptz_2 o1_m2 ON o1_m2.time = o1_m1.time AND o1_m1.device_id = o1_m2.device_id WHERE o1_m2.time < '2000-01-10' AND o1_m1.device_id = 1
) o1 FULL OUTER JOIN (
SELECT o2_m1.time FROM metrics_timestamptz o2_m1 FULL OUTER JOIN metrics_timestamptz_2 o2_m2 ON o2_m2.time = o2_m1.time AND o2_m1.device_id = o2_m2.device_id WHERE o2_m2.time > '2000-01-20' AND o2_m1.device_id = 2
) o2 ON o1.time = o2.time ORDER BY 1,2;
