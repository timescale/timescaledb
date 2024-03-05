-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE TABLE test_security_barrier (time TIMESTAMPTZ NOT NULL, tenant TEXT NOT NULL, data TEXT);
SELECT FROM create_hypertable('test_security_barrier', by_range('time'));

INSERT INTO test_security_barrier(time, tenant, data) VALUES
('2020-01-01', :'ROLE_DEFAULT_PERM_USER','data1'),
('2020-01-01', :'ROLE_DEFAULT_PERM_USER_2','data2');

CREATE VIEW test_security_barrier_view WITH (security_barrier) AS SELECT * FROM test_security_barrier WHERE tenant = current_user;

GRANT SELECT ON test_security_barrier_view TO :ROLE_DEFAULT_PERM_USER;
GRANT SELECT ON test_security_barrier_view TO :ROLE_DEFAULT_PERM_USER_2;

SET ROLE :ROLE_DEFAULT_PERM_USER;
SELECT * FROM test_security_barrier_view;
RESET ROLE;

SET ROLE :ROLE_DEFAULT_PERM_USER_2;
SELECT * FROM test_security_barrier_view;
RESET ROLE;

ALTER TABLE test_security_barrier SET (timescaledb.compress);

-- Compress the chunk
SELECT compress_chunk(show_chunks('test_security_barrier')) IS NOT NULL AS compressed;

SET ROLE :ROLE_DEFAULT_PERM_USER;
SELECT * FROM test_security_barrier_view;
RESET ROLE;

SET ROLE :ROLE_DEFAULT_PERM_USER_2;
SELECT * FROM test_security_barrier_view;
RESET ROLE;

DROP TABLE test_security_barrier CASCADE;

