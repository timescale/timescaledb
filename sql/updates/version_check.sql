-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

DO $$
DECLARE
  catalog_version TEXT;
BEGIN
  SELECT value INTO catalog_version FROM _timescaledb_catalog.metadata WHERE key='timescaledb_version' AND value <> '@START_VERSION@';
  IF FOUND THEN
    RAISE EXCEPTION 'catalog version mismatch'
      USING
        DETAIL = format('current extension version is "%s" but catalog version is "%s"', '@START_VERSION@', catalog_version),
        HINT = 'Make sure the TimescaleDB version used to dump the database is the same as the one used to restore it.';
  END IF;
END$$;

