-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- chunk exclusion on a hash partitioned column with a nondeterministic collation
CREATE COLLATION nocase (provider = icu, locale = 'und-u-ks-level2', deterministic = false);

CREATE TABLE hash_nocase(time timestamptz NOT NULL, device text COLLATE nocase NOT NULL);
SELECT create_hypertable('hash_nocase', 'time', 'device', 4);
INSERT INTO hash_nocase SELECT '2025-01-01', 'device-' || d FROM generate_series(1, 8) d;

SELECT count(*) FROM hash_nocase WHERE device = 'DEVICE-1';
SELECT count(*) FROM hash_nocase WHERE device IN ('DEVICE-1', 'DEVICE-2');

-- a deterministic collation in the query
SELECT count(*) FROM hash_nocase WHERE device = 'device-1' COLLATE "C";
SELECT count(*) FROM hash_nocase WHERE device = 'DEVICE-1' COLLATE "C";

DELETE FROM hash_nocase WHERE device = 'DEVICE-1';
SELECT count(*) FROM hash_nocase;

-- a nondeterministic collation in the query on a column with a deterministic
-- collation
CREATE TABLE hash_default(time timestamptz NOT NULL, device text NOT NULL);
SELECT create_hypertable('hash_default', 'time', 'device', 4);
INSERT INTO hash_default SELECT '2025-01-01', 'device-' || d FROM generate_series(1, 8) d;

SELECT count(*) FROM hash_default WHERE device = 'DEVICE-1' COLLATE nocase;
SELECT count(*) FROM hash_default WHERE device COLLATE nocase IN ('DEVICE-1', 'DEVICE-2');

DELETE FROM hash_default WHERE device = 'DEVICE-1' COLLATE nocase;
SELECT count(*) FROM hash_default;

DROP TABLE hash_nocase;
DROP TABLE hash_default;
DROP COLLATION nocase;

-- chunks are excluded when the query uses the collation of the column
CREATE COLLATION nocase (provider = icu, locale = 'und-u-ks-level2', deterministic = false);

CREATE FUNCTION chunks_scanned(query text) RETURNS int LANGUAGE plpgsql AS
$$
DECLARE
  ln text;
  chunks text[] := '{}';
BEGIN
  FOR ln IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) ' || query
  LOOP
    chunks := chunks || ARRAY(SELECT m[1] FROM regexp_matches(ln, '(_hyper_\d+_\d+_chunk)\M', 'g') m);
  END LOOP;
  RETURN (SELECT count(DISTINCT c) FROM unnest(chunks) c);
END;
$$;

CREATE TABLE hash_nocase(time timestamptz NOT NULL, device text COLLATE nocase NOT NULL);
SELECT create_hypertable('hash_nocase', 'time', 'device', 4);
INSERT INTO hash_nocase SELECT '2025-01-01', 'device-' || d FROM generate_series(1, 8) d;

SELECT chunks_scanned($$SELECT * FROM hash_nocase WHERE device = 'DEVICE-1'$$) = 1;
SELECT count(*) FROM hash_nocase WHERE device = 'DEVICE-1';

SET plan_cache_mode TO force_generic_plan;
PREPARE hash_nocase_device(text) AS SELECT count(*) FROM hash_nocase WHERE device = $1;
SELECT chunks_scanned($$EXECUTE hash_nocase_device('DEVICE-1')$$) = 1;
EXECUTE hash_nocase_device('DEVICE-1');
EXECUTE hash_nocase_device('Device-2');
DEALLOCATE hash_nocase_device;
RESET plan_cache_mode;

SELECT d, (SELECT count(*) FROM hash_nocase WHERE device = 'DEVICE-' || d)
FROM generate_series(1, 4) d;

-- no chunk exclusion for a nondeterministic collation that is not the one of
-- the column
CREATE TABLE hash_default(time timestamptz NOT NULL, device text NOT NULL);
SELECT create_hypertable('hash_default', 'time', 'device', 4);
INSERT INTO hash_default SELECT '2025-01-01', 'device-' || d FROM generate_series(1, 8) d;

SELECT chunks_scanned($$SELECT * FROM hash_default WHERE device = 'DEVICE-1' COLLATE nocase$$) =
  (SELECT count(*) FROM show_chunks('hash_default'));

DROP TABLE hash_nocase;
DROP TABLE hash_default;
DROP FUNCTION chunks_scanned;
DROP COLLATION nocase;

-- name values compared to a text column with a nondeterministic collation
CREATE COLLATION nocase (provider = icu, locale = 'und-u-ks-level2', deterministic = false);

CREATE TABLE hash_nocase(time timestamptz NOT NULL, device text COLLATE nocase NOT NULL);
SELECT create_hypertable('hash_nocase', 'time', 'device', 4);
INSERT INTO hash_nocase SELECT '2025-01-01', 'device-' || d FROM generate_series(1, 8) d;

CREATE TABLE devices(name name);
INSERT INTO devices VALUES ('device-1'), ('DEVICE-2');

SELECT count(*) FROM hash_nocase WHERE device = ('device-1'::name COLLATE nocase);

SELECT d.name, (SELECT count(*) FROM hash_nocase h WHERE h.device = (d.name COLLATE nocase))
FROM devices d ORDER BY 1;

DELETE FROM hash_nocase WHERE device = ('device-1'::name COLLATE nocase);
SELECT count(*) FROM hash_nocase;

DROP TABLE hash_nocase;
DROP TABLE devices;
DROP COLLATION nocase;
