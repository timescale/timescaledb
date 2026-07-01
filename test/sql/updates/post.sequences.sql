-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- check sequences did not get reset. chunk_constraint_name's nextval varies
-- across versions (FK/CHECK tracking was removed) and is excluded.
-- dimension_slice_id_seq is also excluded: each chunk now owns its slices, so
-- the rebuild assigns one id per surviving slice, while a fresh install also
-- burned ids for slices of chunks dropped before the upgrade. Those dropped
-- slices leave no trace to reconstruct, so the next id legitimately differs
-- from a clean install even though the slice rows match.
-- chunk_id_seq is excluded too: a fresh install no longer burns chunk ids for
-- compressed relations, so the next id is lower than in a pre-upgrade catalog.
SELECT seqrelid::regclass,
  CASE WHEN seqrelid::regclass::text IN ('_timescaledb_catalog.chunk_constraint_name',
                                         '_timescaledb_catalog.dimension_slice_id_seq',
                                         '_timescaledb_catalog.chunk_id_seq')
       THEN NULL ELSE nextval(seqrelid) END AS nextval,
  seqstart,
  seqincrement,
  seqmax,
  seqmin
FROM pg_sequence
ORDER BY seqrelid::regclass::text;

