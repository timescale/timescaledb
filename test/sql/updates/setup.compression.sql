-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TYPE custom_type_for_compression AS (high int, low int);

CREATE TABLE compress (
      time                  TIMESTAMPTZ         NOT NULL,
      small_cardinality     TEXT                NULL,
      large_cardinality     TEXT                NULL,
      some_double           DOUBLE PRECISION    NULL,
      some_int              integer             NULL,
      some_custom           custom_type_for_compression         NULL,
      some_bool             boolean             NULL
    );

SELECT table_name FROM create_hypertable( 'compress', 'time');

INSERT INTO compress
SELECT g, 'POR', g::text, 75.0, 40, (1,2)::custom_type_for_compression, true
FROM generate_series('2018-12-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day') g;

INSERT INTO compress
SELECT g, 'POR', NULL, NULL, NULL, NULL, NULL
FROM generate_series('2018-11-01 00:00'::timestamp, '2018-12-31 00:00'::timestamp, '1 day') g;

INSERT INTO compress
SELECT g, 'POR', g::text, 94.0, 45, (3,4)::custom_type_for_compression, true
FROM generate_series('2018-11-01 00:00'::timestamp, '2018-12-15 00:00'::timestamp, '1 day') g;

ALTER TABLE compress SET (timescaledb.compress, timescaledb.compress_segmentby='small_cardinality');

SELECT count(compress_chunk(chunks.chunk_schema|| '.' || chunks.chunk_name)) as count_compressed
FROM timescaledb_information.chunks
WHERE hypertable_name = 'compress' and NOT is_compressed;

\if :WITH_ROLES
GRANT SELECT ON compress TO tsdbadmin;
\endif
