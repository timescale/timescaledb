-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\pset tuples_only on

-- list all extension functions
-- any change in the output of this query requires adjustments
-- in the update and downgrade scripts
-- get_telemetry_report is excluded as it will not be present
-- when built with telemetry disabled
SELECT p.oid::regprocedure::text
FROM pg_proc p
  JOIN pg_depend d ON
    d.objid = p.oid AND
    d.deptype = 'e' AND
    d.refclassid = 'pg_extension'::regclass AND
    d.classid = 'pg_proc'::regclass
  JOIN pg_extension e ON
    e.extname = 'timescaledb' AND
    e.oid = d.refobjid
WHERE proname <> 'get_telemetry_report'
ORDER BY pronamespace::regnamespace::text COLLATE "C", p.oid::regprocedure::text COLLATE "C";
