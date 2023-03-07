-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Test that we can verify constraints on regular tables
CREATE TABLE test_hyper_pk(time TIMESTAMPTZ PRIMARY KEY, temp FLOAT, device INT);
CREATE TABLE test_pk(device INT PRIMARY KEY);
CREATE TABLE test_like(LIKE test_pk);

SELECT create_hypertable('test_hyper_pk', 'time');

\set ON_ERROR_STOP 0
-- Foreign key constraints that reference hypertables are currently unsupported
CREATE TABLE test_fk(time TIMESTAMPTZ REFERENCES test_hyper_pk(time));
\set ON_ERROR_STOP 1

CREATE TABLE test_delete(time timestamp with time zone PRIMARY KEY, temp float);
SELECT create_hypertable('test_delete', 'time');
INSERT INTO test_delete VALUES('2017-01-20T09:00:01', 22.5);
INSERT INTO test_delete VALUES('2017-01-20T09:00:21', 21.2);
INSERT INTO test_delete VALUES('2017-01-20T09:00:47', 25.1);
INSERT INTO test_delete VALUES('2020-01-20T09:00:47', 25.1);
INSERT INTO test_delete VALUES('2021-01-20T09:00:47', 25.1);
SELECT * FROM test_delete WHERE temp = 25.1 ORDER BY time;

CREATE OR replace FUNCTION test_delete_row_count()
RETURNS void AS $$
DECLARE
    v_cnt numeric;
BEGIN
    v_cnt := 0;

        DELETE FROM test_delete WHERE temp = 25.1;
        GET DIAGNOSTICS v_cnt = ROW_COUNT;

        IF v_cnt != 3 THEN
           RAISE EXCEPTION 'unexpected result';
        END IF;
END;
$$ LANGUAGE plpgsql;

SELECT test_delete_row_count();
