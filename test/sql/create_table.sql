-- Test that we can verify constraints on regular tables
CREATE TABLE test_hyper_pk(time TIMESTAMPTZ PRIMARY KEY, temp FLOAT, device INT);
CREATE TABLE test_pk(device INT PRIMARY KEY);
CREATE TABLE test_like(LIKE test_pk);

SELECT create_hypertable('test_hyper_pk', 'time');

\set ON_ERROR_STOP 0
-- Foreign key constraints that reference hypertables are currently unsupported
CREATE TABLE test_fk(time TIMESTAMPTZ REFERENCES test_hyper_pk(time));
\set ON_ERROR_STOP 1
