-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- check sequences did not get reset. chunk_constraint_name's nextval varies
-- across versions (FK/CHECK tracking was removed) and is excluded.
SELECT seqrelid::regclass,
  CASE WHEN seqrelid::regclass::text = '_timescaledb_catalog.chunk_constraint_name'
       THEN NULL ELSE nextval(seqrelid) END AS nextval,
  seqstart,
  seqincrement,
  seqmax,
  seqmin
FROM pg_sequence
ORDER BY seqrelid::regclass::text;

