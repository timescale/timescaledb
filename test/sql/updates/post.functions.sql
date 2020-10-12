-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- check all functions point to the correct library
SELECT
  oid::REGPROCEDURE,
  probin
FROM
  pg_proc
WHERE
  probin LIKE '%timescale%'
ORDER BY
  oid::REGPROCEDURE::TEXT;

