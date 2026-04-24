-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Test for cache pointer use-after-free on invalidation with active pins. We
-- reproduce this by triggering the cache invalidation from a trigger.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE TABLE cache_inval_test(time timestamptz NOT NULL, val int);
SELECT create_hypertable('cache_inval_test', 'time');
INSERT INTO cache_inval_test VALUES ('2024-01-01', 1);

CREATE FUNCTION trg_inval_cache() RETURNS trigger AS $$
BEGIN
  PERFORM set_chunk_time_interval('cache_inval_test', INTERVAL '2 days');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_inval BEFORE INSERT ON cache_inval_test
  FOR EACH ROW EXECUTE FUNCTION trg_inval_cache();

SELECT debug_waitpoint_enable('hypertable-cache-create');

BEGIN;
SAVEPOINT sp;

\set ON_ERROR_STOP 0
INSERT INTO cache_inval_test VALUES ('2024-01-02', 2);
\set ON_ERROR_STOP 1

ROLLBACK TO sp;

INSERT INTO cache_inval_test VALUES ('2024-01-03', 3);
COMMIT;

SELECT val FROM cache_inval_test ORDER BY time;

DROP TABLE cache_inval_test;
