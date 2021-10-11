-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE OR REPLACE FUNCTION show_columns_ext(rel regclass)
RETURNS TABLE("Column" name,
              "Type" text,
              "NotNull" boolean,
              "Compression" text) LANGUAGE SQL STABLE AS
$BODY$
    SELECT a.attname,
    format_type(t.oid, t.typtypmod),
    a.attnotnull,
    (CASE WHEN a.attcompression = 'l' THEN 'lz4' WHEN a.attcompression = 'p' THEN 'pglz' ELSE '' END)
    FROM pg_attribute a, pg_type t
    WHERE a.attrelid = rel
    AND a.atttypid = t.oid
    AND a.attnum >= 0
    ORDER BY a.attnum;
$BODY$;

CREATE TABLE conditions (
  time TIMESTAMP NOT NULL,
  location TEXT NOT NULL,
  temperature DOUBLE PRECISION NULL,
  humidity DOUBLE PRECISION NULL
);

SELECT create_hypertable('conditions', 'time', chunk_time_interval := '1 day'::interval);

INSERT INTO conditions
SELECT generate_series('2021-10-10 00:00'::timestamp, '2021-10-11 00:00'::timestamp, '1 day'), 'POR', 55, 75;

CREATE VIEW t AS
    SELECT 'conditions'::regclass AS r
    UNION ALL
    SELECT * FROM show_chunks('conditions');

SELECT * FROM t, LATERAL show_columns_ext(r) WHERE "Column" = 'location' ORDER BY 1, 2;

ALTER TABLE conditions ALTER COLUMN location SET COMPRESSION pglz;
SELECT * FROM t, LATERAL show_columns_ext(r) WHERE "Column" = 'location' ORDER BY 1, 2;

INSERT INTO conditions VALUES ('2021-10-12 00:00'::timestamp, 'BRA', 66, 77);
SELECT * FROM t, LATERAL show_columns_ext(r) WHERE "Column" = 'location' ORDER BY 1, 2;

ALTER TABLE conditions ALTER COLUMN location SET COMPRESSION default;
SELECT * FROM t, LATERAL show_columns_ext(r) WHERE "Column" = 'location' ORDER BY 1, 2;

\set ON_ERROR_STOP 0
-- failing test because compression is not allowed in "non-TOASTable" datatypes
ALTER TABLE conditions ALTER COLUMN temperature SET COMPRESSION pglz;

SELECT * FROM t, LATERAL show_columns_ext(r) WHERE "Column" = 'temperature' ORDER BY 1, 2;
