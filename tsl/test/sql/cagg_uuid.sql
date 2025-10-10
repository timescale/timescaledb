-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

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

INSERT INTO uuid_events VALUES
       ('0194214e-cd00-7000-a9a7-63f1416dab45', 2, 2.0),
       ('01942117-de80-7000-8121-f12b2b69dd96', 1, 1.0),
       ('0194263e-3a80-7000-8f40-82c987b1bc1f', 3, 3.0),
       ('01942675-2900-7000-8db1-a98694b18785', 4, 4.0),
       ('01942bd2-7380-7000-9bc4-5f97443907b8', 5, 5.0),
       ('01942d52-f900-7000-866e-07d6404d53c1', 6, 6.0);

SELECT * FROM show_chunks('uuid_events');

-- Check that continuous aggregates with UUIDv7 time_bucket is blocked. This
-- will be unblocked later when full cagg support is implemented.
CREATE MATERIALIZED VIEW daily_uuid_events WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', id) AS day, avg(temp)
FROM uuid_events WHERE device < 6
GROUP BY 1;
