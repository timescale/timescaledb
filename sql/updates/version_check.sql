-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

DO $$
DECLARE
  catalog_version TEXT;
BEGIN
  SELECT value INTO catalog_version FROM _timescaledb_catalog.metadata WHERE key='timescaledb_version' AND value <> '@START_VERSION@';
  IF FOUND THEN
    RAISE EXCEPTION 'catalog version mismatch, expected "%" seen "%"', '@START_VERSION@', catalog_version;
  END IF;
END$$;

