-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE SCHEMA hypertable_schema;
GRANT ALL ON SCHEMA hypertable_schema TO :ROLE_DEFAULT_PERM_USER;
SET ROLE :ROLE_DEFAULT_PERM_USER;

CREATE TABLE hypertable_schema.default_perm_user (time timestamptz, temp float, location int);
SELECT create_hypertable('hypertable_schema.default_perm_user', 'time', 'location', 2);
INSERT INTO hypertable_schema.default_perm_user VALUES ('2001-01-01 01:01:01', 23.3, 1);


RESET ROLE;
CREATE TABLE hypertable_schema.superuser (time timestamptz, temp float, location int);
SELECT create_hypertable('hypertable_schema.superuser', 'time', 'location', 2);
INSERT INTO hypertable_schema.superuser VALUES ('2001-01-01 01:01:01', 23.3, 1);

SELECT * FROM _timescaledb_catalog.hypertable ORDER BY id;
SELECT * FROM _timescaledb_catalog.chunk;

DROP OWNED BY :ROLE_DEFAULT_PERM_USER;

SELECT * FROM _timescaledb_catalog.hypertable ORDER BY id;
SELECT * FROM _timescaledb_catalog.chunk;

DROP TABLE  hypertable_schema.superuser;

--everything should be cleaned up
SELECT * FROM _timescaledb_catalog.hypertable GROUP BY id;
SELECT * FROM _timescaledb_catalog.chunk;
SELECT * FROM _timescaledb_catalog.dimension;
SELECT * FROM _timescaledb_catalog.dimension_slice;
SELECT * FROM _timescaledb_catalog.chunk_index;
SELECT * FROM _timescaledb_catalog.chunk_constraint;

-- test drop owned in database without extension installed
\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE database test_drop_owned;
\c test_drop_owned
DROP OWNED BY :ROLE_SUPERUSER;
\c :TEST_DBNAME :ROLE_SUPERUSER
DROP DATABASE test_drop_owned;

-- Test that dependencies on roles are added to chunks when creating
-- new chunks. If that is not done, DROP OWNED BY will not revoke the
-- privilege on the chunk.
CREATE TABLE sensor_data(time timestamptz not null, cpu double precision null);

SELECT * FROM create_hypertable('sensor_data','time');

INSERT INTO sensor_data
SELECT time,
       random() AS cpu
FROM generate_series('2020-01-01'::timestamptz, '2020-01-24'::timestamptz, INTERVAL '10 minute') AS g1(time);

\dp sensor_data
\dp _timescaledb_internal._hyper_3*

GRANT SELECT ON sensor_data TO :ROLE_DEFAULT_PERM_USER;

\dp sensor_data
\dp _timescaledb_internal._hyper_3*

-- Insert more chunks after adding the user to the hypertable. These
-- will now get the privileges of the hypertable.
INSERT INTO sensor_data
SELECT time,
       random() AS cpu
FROM generate_series('2020-01-20'::timestamptz, '2020-02-05'::timestamptz, INTERVAL '10 minute') AS g1(time);

\dp _timescaledb_internal._hyper_3*

-- This should revoke the privileges on both the hypertable and the chunks.
DROP OWNED BY :ROLE_DEFAULT_PERM_USER;

\dp sensor_data
\dp _timescaledb_internal._hyper_3*
