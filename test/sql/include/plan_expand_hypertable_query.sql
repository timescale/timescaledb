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

--use of OR
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


