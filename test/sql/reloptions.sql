-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

CREATE TABLE reloptions_test(time integer, temp float8, color integer)
WITH (fillfactor=75, autovacuum_vacuum_threshold=100);

SELECT create_hypertable('reloptions_test', 'time', chunk_time_interval => 3);

INSERT INTO reloptions_test VALUES (4, 24.3, 1), (9, 13.3, 2);

-- Show that reloptions are inherited by chunks
SELECT relname, reloptions FROM pg_class
WHERE relname ~ '^_hyper.*' AND relkind = 'r';

-- Alter reloptions
ALTER TABLE reloptions_test SET (fillfactor=80, parallel_workers=8);

SELECT relname, reloptions FROM pg_class
WHERE relname ~ '^_hyper.*' AND relkind = 'r';

ALTER TABLE reloptions_test RESET (fillfactor);

SELECT relname, reloptions FROM pg_class
WHERE relname ~ '^_hyper.*' AND relkind = 'r';
