-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Auto-created composite bloom filters that span smallint (int2) columns are
-- dropped by the 2.26.4--2.27.0 migration because the int2 hash changed.
-- Composite bloom filters exist since 2.26.0, so this only runs for baselines
-- that can create them.
CREATE TABLE int2_bloom (
    ts timestamptz NOT NULL,
    a  smallint,
    b  smallint
);

SELECT create_hypertable('int2_bloom', 'ts');

INSERT INTO int2_bloom
SELECT '2021-01-01'::timestamptz + (INTERVAL '1 hour') * i,
       (i % 97)::smallint,
       (i % 53)::smallint
FROM generate_series(1, 5000) i;

-- A multi-column btree index makes the default sparse index a composite bloom
-- filter over the two smallint columns.
CREATE INDEX int2_bloom_a_b_idx ON int2_bloom (a, b);

ALTER TABLE int2_bloom SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'ts'
);

SELECT count(compress_chunk(c)) FROM show_chunks('int2_bloom') c;

-- Same, but with column names long enough that the bloom column name is
-- truncated and suffixed with a hash, exercising that branch of the migration.
CREATE TABLE int2_bloom_long (
    ts timestamptz NOT NULL,
    sensor_measurement_identifier_one smallint,
    sensor_measurement_identifier_two smallint
);

SELECT create_hypertable('int2_bloom_long', 'ts');

INSERT INTO int2_bloom_long
SELECT '2021-01-01'::timestamptz + (INTERVAL '1 hour') * i,
       (i % 97)::smallint,
       (i % 53)::smallint
FROM generate_series(1, 5000) i;

CREATE INDEX int2_bloom_long_idx ON int2_bloom_long
    (sensor_measurement_identifier_one, sensor_measurement_identifier_two);

ALTER TABLE int2_bloom_long SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = '',
    timescaledb.compress_orderby = 'ts'
);

SELECT count(compress_chunk(c)) FROM show_chunks('int2_bloom_long') c;
