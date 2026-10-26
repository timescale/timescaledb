-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_SUPERUSER

SET timezone TO 'America/Los_Angeles';

--
-- Check that drop chunks with a unique constraint works as expected.
--
CREATE TABLE clients (
       id SERIAL PRIMARY KEY,
       name TEXT NOT NULL,
       UNIQUE(name)
);

CREATE TABLE records (
    time TIMESTAMPTZ NOT NULL,
    clientId INT NOT NULL REFERENCES clients(id),
    value DOUBLE PRECISION,
    UNIQUE(time, clientId)
);

SELECT * FROM create_hypertable('records', 'time',
       chunk_time_interval => INTERVAL '1h');

CREATE MATERIALIZED VIEW records_monthly
    WITH (timescaledb.continuous, timescaledb.materialized_only=false)
    AS
        SELECT time_bucket('1d', time) as bucket,
            clientId,
            avg(value) as value_avg,
            max(value)-min(value) as value_spread
        FROM records GROUP BY bucket, clientId WITH NO DATA;

INSERT INTO clients(name) VALUES ('test-client');

INSERT INTO records
SELECT generate_series('2000-03-01'::timestamptz,'2000-04-01','1 day'),1,3.14;

SELECT * FROM records_monthly ORDER BY bucket, clientId;

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'records_monthly' ORDER BY range_start;

SELECT chunk_name, range_start, range_end
FROM timescaledb_information.chunks
WHERE hypertable_name = 'records' ORDER BY range_start;

CALL refresh_continuous_aggregate('records_monthly', NULL, NULL);

\set VERBOSITY default
SELECT drop_chunks('records', '2000-03-16'::timestamptz);

\set VERBOSITY terse
DROP MATERIALIZED VIEW records_monthly;
DROP TABLE records;
DROP TABLE clients;
\set VERBOSITY default

CREATE PROCEDURE refresh_cagg_by_chunk_range(_cagg REGCLASS, _hypertable REGCLASS, _older_than INTEGER)
AS
$$
DECLARE
    _r RECORD;
BEGIN
    WITH _chunks AS (
        SELECT relname, nspname
        FROM show_chunks(_hypertable, _older_than) AS relid
        JOIN pg_catalog.pg_class ON pg_class.oid = relid AND pg_class.relkind = 'r'
        JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
    )
    SELECT MIN(range_start) AS range_start, MAX(range_end) AS range_end
    INTO _r
    FROM
        _chunks
        JOIN _timescaledb_catalog.chunk ON chunk.relid = format('%I.%I', _chunks.nspname, _chunks.relname)::regclass
        JOIN _timescaledb_catalog.dimension_slice ON dimension_slice.chunk_id = chunk.id;

    RAISE INFO 'range_start=% range_end=%', _r.range_start::int, _r.range_end::int;
    CALL refresh_continuous_aggregate(_cagg, _r.range_start::int, _r.range_end::int + 1);
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_int_now() returns INT LANGUAGE SQL STABLE as
    $$ SELECT 125 $$;

CREATE TABLE conditions(time_int INT NOT NULL, value FLOAT);
SELECT create_hypertable('conditions', 'time_int', chunk_time_interval => 4);

INSERT INTO conditions
SELECT time_val, 1 FROM generate_series(0, 19, 1) AS time_val;

SELECT set_integer_now_func('conditions', 'test_int_now');

CREATE MATERIALIZED VIEW conditions_2
    WITH (timescaledb.continuous, timescaledb.materialized_only = TRUE)
    AS
        SELECT time_bucket(2, time_int) as bucket,
            SUM(value), COUNT(value)
        FROM conditions GROUP BY bucket WITH DATA;

SELECT * FROM conditions_2 ORDER BY bucket;

UPDATE conditions SET value = 4.00 WHERE time_int = 0;
UPDATE conditions SET value = 4.00 WHERE time_int = 6;

CALL refresh_cagg_by_chunk_range('conditions_2', 'conditions', 4);
SELECT drop_chunks('conditions', 4);

SELECT * FROM conditions_2 ORDER BY bucket;

CALL refresh_cagg_by_chunk_range('conditions_2', 'conditions', 8);
SELECT * FROM conditions_2 ORDER BY bucket;

UPDATE conditions SET value = 4.00 WHERE time_int = 19;

SELECT drop_chunks('conditions', 8);
CALL refresh_cagg_by_chunk_range('conditions_2', 'conditions', 12);
SELECT * FROM conditions_2 ORDER BY bucket;

CALL refresh_cagg_by_chunk_range('conditions_2', 'conditions', NULL);
SELECT * FROM conditions_2 ORDER BY bucket;

DROP PROCEDURE refresh_cagg_by_chunk_range(REGCLASS, REGCLASS, INTEGER);

-- DROP TABLE on a chunk invalidates the cagg with and without schema qualification
CREATE TABLE drop_inval(time int NOT NULL, value int);
SELECT table_name FROM create_hypertable('drop_inval', 'time', chunk_time_interval => 10);
CREATE FUNCTION drop_inval_now() RETURNS int LANGUAGE SQL STABLE AS 'SELECT 100';
SELECT set_integer_now_func('drop_inval', 'drop_inval_now');

CREATE MATERIALIZED VIEW drop_inval_cagg
    WITH (timescaledb.continuous, timescaledb.materialized_only = true)
    AS SELECT time_bucket(10, time) AS bucket, count(*) FROM drop_inval GROUP BY 1
    WITH NO DATA;

INSERT INTO drop_inval SELECT g, g FROM generate_series(0, 29) g;
CALL refresh_continuous_aggregate('drop_inval_cagg', NULL, NULL);

SELECT id AS drop_inval_id FROM _timescaledb_catalog.hypertable
WHERE table_name = 'drop_inval' \gset

SELECT chunk_schema AS chunk_schema_1, chunk_name AS chunk_name_1
FROM timescaledb_information.chunks
WHERE hypertable_name = 'drop_inval' AND range_start_integer = 0 \gset
SELECT chunk_name AS chunk_name_2
FROM timescaledb_information.chunks
WHERE hypertable_name = 'drop_inval' AND range_start_integer = 10 \gset

SELECT lowest_modified_value, greatest_modified_value
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = :drop_inval_id ORDER BY 1;

DROP TABLE :chunk_schema_1.:chunk_name_1;

SET search_path TO :chunk_schema_1, public;
DROP TABLE :chunk_name_2;
RESET search_path;

SELECT lowest_modified_value, greatest_modified_value
FROM _timescaledb_catalog.continuous_aggs_hypertable_invalidation_log
WHERE hypertable_id = :drop_inval_id ORDER BY 1;

DROP MATERIALIZED VIEW drop_inval_cagg;
DROP TABLE drop_inval;
DROP FUNCTION drop_inval_now();
