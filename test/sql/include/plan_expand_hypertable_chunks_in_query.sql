-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--we want to see how our logic excludes chunks
--and not how much work constraint_exclusion does
SET constraint_exclusion = 'off';

:PREFIX SELECT * FROM hyper ORDER BY value;
-- explicit chunk exclusion
:PREFIX SELECT * FROM hyper WHERE _timescaledb_functions.chunks_in(hyper, ARRAY[1,2]) ORDER BY value;
:PREFIX SELECT * FROM (SELECT * FROM hyper h WHERE _timescaledb_functions.chunks_in(h, ARRAY[1,2,3])) T ORDER BY value;
:PREFIX SELECT * FROM hyper WHERE _timescaledb_functions.chunks_in(hyper, ARRAY[1,2,3]) AND time < 10 ORDER BY value;
:PREFIX SELECT * FROM hyper_ts WHERE device_id = 'dev1' AND time < to_timestamp(10) AND _timescaledb_functions.chunks_in(hyper_ts, ARRAY[116]) ORDER BY value;
:PREFIX SELECT * FROM hyper_ts h JOIN tag on (h.tag_id = tag.id ) WHERE _timescaledb_functions.chunks_in(h, ARRAY[116]) AND time < to_timestamp(10) AND device_id = 'dev1' ORDER BY value;
:PREFIX SELECT * FROM hyper_w_space h1 JOIN hyper_ts h2 ON h1.device_id=h2.device_id WHERE _timescaledb_functions.chunks_in(h1, ARRAY[104,105]) AND _timescaledb_functions.chunks_in(h2, ARRAY[116,117]) ORDER BY h1.value;
:PREFIX SELECT * FROM hyper_w_space h1 JOIN hyper_ts h2 ON h1.device_id=h2.device_id AND _timescaledb_functions.chunks_in(h2, ARRAY[116,117]) WHERE _timescaledb_functions.chunks_in(h1, ARRAY[104,105]) ORDER BY h1.value;
:PREFIX SELECT * FROM hyper h1, hyper h2 WHERE _timescaledb_functions.chunks_in(h1, ARRAY[1,2]) AND _timescaledb_functions.chunks_in(h2, ARRAY[2,3]);
SET enable_seqscan=false;
-- Should perform index-only scan. Since we pass whole row into the function it might block planner from using index-only scan.
-- But since we'll remove the function from the query tree before planner decision it shouldn't affect index-only decision.
:PREFIX SELECT time FROM hyper WHERE time=0 AND _timescaledb_functions.chunks_in(hyper, ARRAY[1]);
:PREFIX SELECT first(value, time) FROM hyper h WHERE _timescaledb_functions.chunks_in(h, ARRAY[1]);

\set ON_ERROR_STOP 0
SELECT * FROM hyper WHERE _timescaledb_functions.chunks_in(hyper, ARRAY[1,2]) AND _timescaledb_functions.chunks_in(hyper, ARRAY[2,3]);
SELECT * FROM hyper WHERE _timescaledb_functions.chunks_in(2, ARRAY[1]);
SELECT * FROM hyper WHERE time < 10 OR _timescaledb_functions.chunks_in(hyper, ARRAY[1,2]);
SELECT _timescaledb_functions.chunks_in(hyper, ARRAY[1,2]) FROM hyper;
-- non existing chunk id
SELECT * FROM hyper WHERE _timescaledb_functions.chunks_in(hyper, ARRAY[123456789]);
-- chunk that belongs to another hypertable
SELECT * FROM hyper WHERE _timescaledb_functions.chunks_in(hyper, ARRAY[104]);
-- passing wrong row ref
SELECT * FROM hyper WHERE _timescaledb_functions.chunks_in(ROW(1,2), ARRAY[104]);
-- passing func as chunk id
SELECT * FROM hyper h WHERE _timescaledb_functions.chunks_in(h, array_append(ARRAY[1],current_setting('server_version_num')::int));
-- NULL chunk IDs not allowed in chunk array
SELECT * FROM hyper h WHERE _timescaledb_functions.chunks_in(h, ARRAY[NULL::int]);
\set ON_ERROR_STOP 1

-- chunks_in is STRICT function and for NULL arguments a null result is returned
SELECT * FROM hyper h WHERE _timescaledb_functions.chunks_in(h, NULL);
