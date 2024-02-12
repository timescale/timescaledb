-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TYPE custom_type_for_compression AS (high int, low int);

CREATE TABLE compress (
      time                  TIMESTAMPTZ         NOT NULL,
      small_cardinality     TEXT                NULL,
      large_cardinality     TEXT                NULL,
      dropped               TEXT                NULL,
      some_double           DOUBLE PRECISION    NULL,
      some_int              integer             NULL,
      some_custom           custom_type_for_compression         NULL,
      some_bool             boolean             NULL
    );

SELECT table_name FROM create_hypertable( 'compress', 'time');

INSERT INTO compress
SELECT g, 'POR', g::text, 'lint', 75.0, 40, (1,2)::custom_type_for_compression, true
FROM generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day') g;

INSERT INTO compress
SELECT g, 'POR', NULL, 'lint', NULL, NULL, NULL, NULL
FROM generate_series('2018-11-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day') g;

INSERT INTO compress
SELECT g, 'POR', g::text, 'lint', 94.0, 45, (3,4)::custom_type_for_compression, true
FROM generate_series('2018-11-01 00:00'::timestamp, '2018-12-15 00:00'::timestamp, '1 day') g;

DO $$
DECLARE
  ts_minor int := (SELECT (string_to_array(extversion,'.'))[2]::int FROM pg_extension WHERE extname = 'timescaledb');
BEGIN

  -- support for dropping columns on compressed hypertables was added in 2.10.0
  -- so we drop before compressing if the version is before 2.10.0
  IF ts_minor < 10 THEN
    ALTER TABLE compress DROP COLUMN dropped;
  END IF;

  ALTER TABLE compress SET (timescaledb.compress, timescaledb.compress_segmentby='small_cardinality');
  PERFORM count(compress_chunk(ch, true)) FROM show_chunks('compress') ch;

  IF ts_minor >= 10 THEN
    ALTER TABLE compress DROP COLUMN dropped;
  END IF;
END
$$;

\if :WITH_ROLES
GRANT SELECT ON compress TO tsdbadmin;
\endif
