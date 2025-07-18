-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE PROCEDURE
       convert_hypertable_to_columnstore(regclass)
LANGUAGE plpgsql AS $$
DECLARE
  chunk REGCLASS;
BEGIN
   FOR chunk IN SELECT show_chunks($1)
   LOOP
      CALL convert_to_columnstore(chunk);
   END LOOP;
END
$$;

CREATE PROCEDURE
       convert_hypertable_to_rowstore(regclass)
LANGUAGE plpgsql AS $$
DECLARE
  chunk REGCLASS;
BEGIN
   FOR chunk IN SELECT show_chunks($1)
   LOOP
      CALL convert_to_rowstore(chunk);
   END LOOP;
END
$$;

-- These are mostly taken from compression_ddl.sql and are only
-- intended to check that aliases work. In that sense, the actual
-- result of each query is not particularly important.
CREATE TABLE test1 (ts timestamptz, i integer, b bigint, t text);
SELECT * FROM create_hypertable('test1', 'ts');
INSERT INTO test1 SELECT t,  random(), random(), random()::text
FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-28 1:00', '1 hour') t;
ALTER TABLE test1 set (
      timescaledb.enable_columnstore,
      timescaledb.segmentby = 'b',
      timescaledb.orderby = 'ts desc'
);

CALL convert_hypertable_to_columnstore('test1');
CALL convert_hypertable_to_rowstore('test1');
CALL convert_hypertable_to_columnstore('test1');

-- Pick one chunk to play with and test option names. We mostly use
-- default since we are only interested in that the option names are
-- accepted.
SELECT chunk FROM show_chunks('test1') tbl(chunk) LIMIT 1 \gset
CALL convert_to_rowstore(:'chunk', if_columnstore => true);
CALL convert_to_columnstore(:'chunk',
     if_not_columnstore => true,
     recompress => false);

CALL add_columnstore_policy('test1', interval '1 day');
CALL remove_columnstore_policy('test1');

SELECT * FROM timescaledb_information.hypertable_columnstore_settings;
SELECT * FROM timescaledb_information.chunk_columnstore_settings ORDER BY chunk;

VACUUM FULL test1;

-- We only care about the column names for the result. They should be
-- the same as for the original function.
SELECT * FROM chunk_columnstore_stats('test1') where 1 = 2 order by chunk_name;
SELECT * FROM hypertable_columnstore_stats('test1') where 1 = 2;

CREATE TABLE test2 (ts timestamptz, i integer, b bigint, t text);
SELECT * FROM create_hypertable('test2', 'ts');
INSERT INTO test2 SELECT t,  random(), random(), random()::text
FROM generate_series('2018-03-02 1:00'::TIMESTAMPTZ, '2018-03-28 1:00', '1 hour') t;
ALTER TABLE test2 set (
      timescaledb.columnstore,
      timescaledb.segmentby = 'b',
      timescaledb.orderby = 'ts desc'
);
SELECT * FROM timescaledb_information.hypertables WHERE hypertable_name = 'test2';


