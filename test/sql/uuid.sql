-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--
--
-- Test "time" partitioning on UUIDv7
--
--
CREATE TABLE uuid_events(id uuid primary key, device int, temp float);

\set ON_ERROR_STOP 0
-- Test invalid interval type
SELECT create_hypertable('uuid_events', 'id', chunk_time_interval => true);
\set ON_ERROR_STOP 1

SELECT create_hypertable('uuid_events', 'id', chunk_time_interval => interval '1 day');


--
-- Test that inserting boundary values generates the right constraints
-- on chunks.
--

-- First value with min time: 00000000-0000-7000-8000-000000000000
BEGIN;
INSERT INTO uuid_events VALUES ('00000000-0000-7000-8000-000000000000', 1, 1.0);
SELECT (test.show_constraints(ch)).* from show_chunks('uuid_events') ch;
SELECT _timescaledb_functions.timestamptz_from_uuid_v7(id), device, temp
FROM uuid_events ORDER BY id;

-- Update v7 UUID to a v4 UUID that doesn't violate the chunk's range
-- constraint. Currently we don't prevent this "loophole".
UPDATE uuid_events SET id = '00000000-0001-4000-8000-000000000000'
WHERE id = '00000000-0000-7000-8000-000000000000';

SELECT _timescaledb_functions.timestamptz_from_uuid_v7(id), device, temp
FROM uuid_events ORDER BY id;

-- Update v7 UUID to a v4 that violates the chunk constraint:
\set ON_ERROR_STOP 0
UPDATE uuid_events SET id = 'ffff0000-0000-4000-8000-000000000000'
WHERE id = '00000000-0001-4000-8000-000000000000';
\set ON_ERROR_STOP 1

ROLLBACK;

-- Last value with min time: 00000000-0000-7fff-bfff-ffffffffffff
BEGIN;
INSERT INTO uuid_events VALUES ('00000000-0000-7fff-bfff-ffffffffffff', 1, 1.0);
SELECT (test.show_constraints(ch)).* from show_chunks('uuid_events') ch;
ROLLBACK;

-- First value with max time: ffffffff-ffff-7000-8000-000000000000
BEGIN;
INSERT INTO uuid_events VALUES ('ffffffff-ffff-7000-8000-000000000000', 1, 1.0);
SELECT (test.show_constraints(ch)).* from show_chunks('uuid_events') ch;
ROLLBACK;

-- (Max time with min value) + 1
BEGIN;
INSERT INTO uuid_events VALUES ('ffffffff-ffff-7000-8000-000000000001', 1, 1.0);
SELECT (test.show_constraints(ch)).* from show_chunks('uuid_events') ch;
ROLLBACK;

-- Last value with max time: ffffffff-ffff-7fff-bfff-ffffffffffff
BEGIN;
INSERT INTO uuid_events VALUES ('ffffffff-ffff-7fff-bfff-ffffffffffff', 1, 1.0);
SELECT (test.show_constraints(ch)).* from show_chunks('uuid_events') ch;
ROLLBACK;

--
-- It is possible to generate UUIDs like follows, but the random
-- generator used doesn't respect setseed() so used constant UUIDs for
-- determinism.
--
-- (_timescaledb_functions.uuid_v7_from_timestamptz('2025-01-01 01:00 PST'), 1, 1.0),
-- (_timescaledb_functions.uuid_v7_from_timestamptz('2025-01-01 02:00 PST'), 2, 2.0),
-- (_timescaledb_functions.uuid_v7_from_timestamptz('2025-01-02 01:00 PST'), 3, 3.0),
-- (_timescaledb_functions.uuid_v7_from_timestamptz('2025-01-02 02:00 PST'), 4, 4.0),
-- (_timescaledb_functions.uuid_v7_from_timestamptz('2025-01-03 03:00 PST'), 5, 5.0),
-- (_timescaledb_functions.uuid_v7_from_timestamptz('2025-01-03 10:00 PST'), 6, 6.0);
--
INSERT INTO uuid_events VALUES
       ('0194214e-cd00-7000-a9a7-63f1416dab45', 2, 2.0),
       ('01942117-de80-7000-8121-f12b2b69dd96', 1, 1.0),
       ('0194263e-3a80-7000-8f40-82c987b1bc1f', 3, 3.0),
       ('01942675-2900-7000-8db1-a98694b18785', 4, 4.0),
       ('01942bd2-7380-7000-9bc4-5f97443907b8', 5, 5.0),
       ('01942d52-f900-7000-866e-07d6404d53c1', 6, 6.0);

SELECT * FROM show_chunks('uuid_events');

SELECT (test.show_constraints(ch)).* from show_chunks('uuid_events') ch;
SELECT id, device, temp FROM uuid_events;

SELECT _timescaledb_functions.timestamptz_from_uuid_v7(id), device, temp
FROM uuid_events;

SELECT _timescaledb_functions.timestamptz_from_uuid_v7(id), device, temp
FROM uuid_events ORDER BY id;

SELECT
    _timescaledb_functions.to_timestamp(range_start) AS range_start,
    _timescaledb_functions.to_timestamp(range_end) AS range_end
FROM _timescaledb_catalog.dimension_slice ds
JOIN _timescaledb_catalog.dimension d ON (ds.dimension_id = d.id)
JOIN _timescaledb_catalog.hypertable h ON (d.hypertable_id = h.id)
WHERE h.table_name = 'uuid_events';

SELECT
    _timescaledb_functions.to_timestamp(range_start) AS chunk_range_start,
    _timescaledb_functions.to_timestamp(range_end) AS chunk_range_end
FROM _timescaledb_catalog.dimension_slice ds
JOIN _timescaledb_catalog.dimension d ON (ds.dimension_id = d.id)
JOIN _timescaledb_catalog.hypertable h ON (d.hypertable_id = h.id)
WHERE h.table_name = 'uuid_events'
LIMIT 1 OFFSET 1 \gset

-- Test that chunk exclusion on uuidv7 column works
SELECT :'chunk_range_start',  _timescaledb_functions.uuid_v7_from_timestamptz_zeroed(:'chunk_range_start');

-- Exclude all but one chunk
EXPLAIN (verbose, costs off, timing off)
SELECT _timescaledb_functions.timestamptz_from_uuid_v7(id), device, temp
FROM uuid_events WHERE id < _timescaledb_functions.uuid_v7_from_timestamptz_zeroed(:'chunk_range_start');

SELECT _timescaledb_functions.timestamptz_from_uuid_v7(id), device, temp
FROM uuid_events WHERE id < _timescaledb_functions.uuid_v7_from_timestamptz_zeroed(:'chunk_range_start');

-- Exclude only one chunk. Add ordering (DESC)
EXPLAIN (verbose, costs off, timing off)
SELECT _timescaledb_functions.timestamptz_from_uuid_v7(id), device, temp
FROM uuid_events WHERE id < _timescaledb_functions.uuid_v7_from_timestamptz_zeroed(:'chunk_range_end')
ORDER BY id DESC;

SELECT _timescaledb_functions.timestamptz_from_uuid_v7(id), device, temp
FROM uuid_events WHERE id < _timescaledb_functions.uuid_v7_from_timestamptz_zeroed(:'chunk_range_end')
ORDER BY id DESC;

-- Insert non-v7 UUIDs
\set ON_ERROR_STOP 0
INSERT INTO uuid_events SELECT 'a8961135-cd89-4c4b-aa05-79df642407dd', 5, 5.0;
\set ON_ERROR_STOP 1

DROP TABLE uuid_events;
