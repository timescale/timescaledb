-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\set ON_ERROR_STOP 0

\qecho test timestamp upper boundary
\qecho this will error because even though the transformation results in a valid timestamp
\qecho our supported range of values for time is smaller then postgres
:PREFIX SELECT * FROM metrics_timestamp WHERE time_bucket('1d',time) < '294276-01-01'::timestamp ORDER BY time;
\qecho transformation would be out of range

\qecho test timestamptz upper boundary
\qecho this will error because even though the transformation results in a valid timestamp
\qecho our supported range of values for time is smaller then postgres
:PREFIX SELECT time FROM metrics_timestamptz WHERE time_bucket('1d',time) < '294276-01-01'::timestamptz ORDER BY time;

\set ON_ERROR_STOP 1
