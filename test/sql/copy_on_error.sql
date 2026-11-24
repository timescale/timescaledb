-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- ON_ERROR was introduced in PostgreSQL 17.
CREATE TABLE copy_ignore (time bigint NOT NULL, val int);
SELECT create_hypertable('copy_ignore', 'time', chunk_time_interval => 10);

-- Rejected rows must not be buffered or create chunks. Valid rows following
-- conversion errors must still be parsed and routed to their own chunks.
COPY copy_ignore FROM STDIN WITH (FORMAT csv, ON_ERROR ignore);
1,1
2,not_a_number
11,3
21,bad_value
31,5
\.
SELECT * FROM copy_ignore ORDER BY time;
SELECT count(*), sum(val) FROM copy_ignore;
SELECT count(*) AS chunks FROM show_chunks('copy_ignore');

-- Errors in the partition column must be skipped before chunk routing, too.
-- No rows or chunks should be added when all input rows are rejected.
COPY copy_ignore FROM STDIN WITH (FORMAT csv, ON_ERROR ignore);
41,bad_value
bad_time,7
\.
SELECT count(*), sum(val) FROM copy_ignore;
SELECT count(*) AS chunks FROM show_chunks('copy_ignore');

-- A BEFORE trigger selects the unbuffered insert path. Rejected rows must
-- reach neither the WHERE expression (which would divide by zero) nor triggers.
TRUNCATE copy_ignore;
CREATE TABLE copy_ignore_trigger_rows (time bigint, val int, skipped bigint);
CREATE FUNCTION copy_ignore_trigger() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_stat_clear_snapshot();
    INSERT INTO copy_ignore_trigger_rows
        SELECT NEW.time, NEW.val, tuples_skipped
        FROM pg_stat_progress_copy WHERE pid = pg_backend_pid();
    RETURN NEW;
END
$$;
CREATE TRIGGER copy_ignore_before BEFORE INSERT ON copy_ignore
FOR EACH ROW EXECUTE FUNCTION copy_ignore_trigger();

COPY copy_ignore FROM STDIN WITH (FORMAT csv, ON_ERROR ignore)
WHERE 10 / val > 0;
1,1
2,not_a_number
bad_time,2
11,3
21,bad_value
31,5
\.
SELECT * FROM copy_ignore ORDER BY time;
SELECT * FROM copy_ignore_trigger_rows ORDER BY time;
SELECT count(*) AS chunks FROM show_chunks('copy_ignore');

-- Ordinary errors must still abort the COPY statement and roll back its rows.
TRUNCATE copy_ignore, copy_ignore_trigger_rows;
ALTER TABLE copy_ignore ADD CHECK (val > 0), ALTER COLUMN val SET NOT NULL;
\set ON_ERROR_STOP 0
\set VERBOSITY sqlstate
-- The default ON_ERROR behavior is unchanged.
COPY copy_ignore FROM STDIN WITH (FORMAT csv);
1,1
2,not_a_number
11,3
\.
SELECT count(*) FROM copy_ignore;

-- ON_ERROR ignore only handles conversion errors, not CHECK/NOT NULL failures
-- or malformed records.
COPY copy_ignore FROM STDIN WITH (FORMAT csv, ON_ERROR ignore);
1,1
2,-1
\.
SELECT count(*) FROM copy_ignore;
COPY copy_ignore FROM STDIN WITH (FORMAT csv, ON_ERROR ignore);
1,1
2,
\.
SELECT count(*) FROM copy_ignore;
COPY copy_ignore FROM STDIN WITH (FORMAT csv, ON_ERROR ignore);
1,1
2,2,extra
\.
SELECT count(*) FROM copy_ignore;
SELECT count(*) FROM copy_ignore_trigger_rows;
\set VERBOSITY default
\set ON_ERROR_STOP 1

DROP TABLE copy_ignore, copy_ignore_trigger_rows;
DROP FUNCTION copy_ignore_trigger();
