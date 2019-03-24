-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- create a now() function for repeatable testing that always returns
-- the same timestamp. It needs to be marked STABLE
CREATE OR REPLACE FUNCTION now_s()
RETURNS timestamptz LANGUAGE PLPGSQL STABLE AS
$BODY$
BEGIN
    RAISE NOTICE 'Stable function now_s() called!';
    RETURN '2017-08-22T10:00:00'::timestamptz;
END;
$BODY$;

CREATE OR REPLACE FUNCTION now_i()
RETURNS timestamptz LANGUAGE PLPGSQL IMMUTABLE AS
$BODY$
BEGIN
    RAISE NOTICE 'Immutable function now_i() called!';
    RETURN '2017-08-22T10:00:00'::timestamptz;
END;
$BODY$;

CREATE OR REPLACE FUNCTION now_v()
RETURNS timestamptz LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    RAISE NOTICE 'Volatile function now_v() called!';
    RETURN '2017-08-22T10:00:00'::timestamptz;
END;
$BODY$;

CREATE TABLE append_test(time timestamptz, temp float, colorid integer);
SELECT create_hypertable('append_test', 'time', chunk_time_interval => 2628000000000);

-- create three chunks
INSERT INTO append_test VALUES ('2017-03-22T09:18:22', 23.5, 1),
                               ('2017-03-22T09:18:23', 21.5, 1),
                               ('2017-05-22T09:18:22', 36.2, 2),
                               ('2017-05-22T09:18:23', 15.2, 2),
                               ('2017-08-22T09:18:22', 34.1, 3);

-- Create another hypertable to join with
CREATE TABLE join_test(time timestamptz, temp float, colorid integer);
SELECT create_hypertable('join_test', 'time', chunk_time_interval => 2628000000000);

INSERT INTO join_test VALUES ('2017-01-22T09:18:22', 15.2, 1),
                             ('2017-02-22T09:18:22', 24.5, 2),
                             ('2017-08-22T09:18:22', 23.1, 3);

