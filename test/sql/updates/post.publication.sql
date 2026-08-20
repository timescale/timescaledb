-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- After the upgrade the recreated ddl_command_end event trigger must fire for
-- CREATE PUBLICATION, so that FOR TABLES IN SCHEMA backfills the hypertable's
-- chunks (they live in _timescaledb_internal and are missed by PostgreSQL).
\if :WITH_SUPERUSER
SET timescaledb.enable_chunk_auto_publication = true;
CREATE PUBLICATION up_pub FOR TABLES IN SCHEMA update_pub_schema;
-- Both pre-upgrade chunks must get explicit publication rows.
SELECT count(*) AS published_chunks FROM pg_publication_rel r
  JOIN pg_publication p ON p.oid = r.prpubid
  JOIN pg_class c ON c.oid = r.prrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE p.pubname = 'up_pub' AND n.nspname = '_timescaledb_internal';
DROP PUBLICATION up_pub;
\endif
