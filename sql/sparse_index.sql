-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION _timescaledb_functions.bloom1_contains(_timescaledb_internal.bloom1, anyelement)
RETURNS bool
AS '@MODULE_PATHNAME@', 'ts_bloom1_contains'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.jsonb_get_matching_index_entry(
    config jsonb,
    attr_name text,
    target_type text
) RETURNS jsonb AS $$
DECLARE
    elem jsonb;
    attr_count int := 0;
BEGIN
  -- Return NULL if any input is NULL
  IF config IS NULL OR attr_name IS NULL OR target_type IS NULL THEN
    RETURN NULL;
  END IF;

  FOR elem IN SELECT * FROM jsonb_array_elements(config)
  LOOP
    IF elem->>'column' =  attr_name THEN
      attr_count := attr_count + 1;

      IF elem->>'type' = target_type THEN
        IF attr_count > 2 THEN
          RAISE EXCEPTION 'Found % sparse index entries for attribute "%"', attr_count, attr_name;
        END IF;
        RETURN elem;
      END IF;
    END IF;
  END LOOP;

  IF attr_count > 2 THEN
    RAISE EXCEPTION 'Found % sparse index entries for attribute "%"', attr_count, attr_name;
  END IF;

  RETURN NULL;
END;
$$ LANGUAGE PLPGSQL
SET search_path TO pg_catalog, pg_temp;
