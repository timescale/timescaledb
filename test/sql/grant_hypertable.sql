-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE TABLE conditions(
    time TIMESTAMPTZ NOT NULL,
    device INTEGER,
    temperature FLOAT
);

-- Create a hypertable and show that it does not have any privileges
SELECT * FROM create_hypertable('conditions', 'time', chunk_time_interval => '5 days'::interval);
INSERT INTO conditions
SELECT time, (random()*30)::int, random()*80 - 40
FROM generate_series('2018-12-01 00:00'::timestamp, '2018-12-10 00:00'::timestamp, '1h') AS time;
\z conditions
\z _timescaledb_internal.*chunk

-- Add privileges and show that they propagate to the chunks
GRANT SELECT, INSERT ON conditions TO PUBLIC;
\z conditions
\z _timescaledb_internal.*chunk

-- Create some more chunks and show that they also get the privileges.
INSERT INTO conditions
SELECT time, (random()*30)::int, random()*80 - 40
FROM generate_series('2018-12-10 00:00'::timestamp, '2018-12-20 00:00'::timestamp, '1h') AS time;
\z conditions
\z _timescaledb_internal.*chunk

-- Revoke one of the privileges and show that it propagate to the
-- chunks.
REVOKE INSERT ON conditions FROM PUBLIC;
\z conditions
\z _timescaledb_internal.*chunk

-- Add some more chunks and show that it inherits the grants from the
-- hypertable.
INSERT INTO conditions
SELECT time, (random()*30)::int, random()*80 - 40
FROM generate_series('2018-12-20 00:00'::timestamp, '2018-12-30 00:00'::timestamp, '1h') AS time;
\z conditions
\z _timescaledb_internal.*chunk

-- Change grants of one chunk explicitly and check that it is possible
\z _timescaledb_internal._hyper_1_1_chunk
GRANT UPDATE ON _timescaledb_internal._hyper_1_1_chunk TO PUBLIC;
\z _timescaledb_internal._hyper_1_1_chunk
REVOKE SELECT ON _timescaledb_internal._hyper_1_1_chunk FROM PUBLIC;
\z _timescaledb_internal._hyper_1_1_chunk

-- Check that revoking a permission first on the chunk and then on the
-- hypertable that was added through the hypertable (INSERT and
-- SELECT, in this case) still do not copy permissions from the
-- hypertable (so there should not be a select permission to public on
-- the chunk but there should be one on the hypertable).
GRANT INSERT ON conditions TO PUBLIC;
\z conditions
\z _timescaledb_internal._hyper_1_2_chunk
REVOKE SELECT ON _timescaledb_internal._hyper_1_2_chunk FROM PUBLIC;
REVOKE INSERT ON conditions FROM PUBLIC;
\z conditions
\z _timescaledb_internal._hyper_1_2_chunk

-- Check that granting permissions through hypertable does not remove
-- separate grants on chunk.
GRANT UPDATE ON _timescaledb_internal._hyper_1_3_chunk TO PUBLIC;
\z conditions
\z _timescaledb_internal._hyper_1_3_chunk
GRANT INSERT ON conditions TO PUBLIC;
REVOKE INSERT ON conditions FROM PUBLIC;
\z conditions
\z _timescaledb_internal._hyper_1_3_chunk
