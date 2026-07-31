-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- create hypertable with 500 chunks to detect excessive locking in extension update
CREATE TABLE lock_test(time timestamptz NOT NULL, device text, value float, PRIMARY KEY(time, device)) WITH (tsdb.hypertable);

INSERT INTO lock_test SELECT '2000-01-01'::timestamptz + format('%s week', i)::interval, i::text, i FROM generate_series(1, 500) g(i);

SELECT compress_chunk(show_chunks('lock_test'));


