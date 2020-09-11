-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- check sequences did not get reset
SELECT seqrelid::regclass,
  nextval(seqrelid),
  seqstart,
  seqincrement,
  seqmax,
  seqmin
FROM pg_sequence
ORDER BY seqrelid::regclass::text;

