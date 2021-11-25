-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME
--list all extension functions in public schema
SELECT DISTINCT proname
FROM pg_proc
WHERE oid IN (
    SELECT objid
    FROM pg_catalog.pg_depend
    WHERE refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
      AND refobjid = (
        SELECT oid
        FROM pg_extension
        WHERE extname = 'timescaledb')
        AND deptype = 'e'
        AND classid = 'pg_catalog.pg_proc'::regclass)
    AND pronamespace = 'public'::regnamespace
    AND proname NOT LIKE '%telemetry%'
  ORDER BY proname;

