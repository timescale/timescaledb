-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ANALYZE 'EXPLAIN (analyze, costs off, timing off, summary off)'

-- test constraint exclusion with prepared statements and generic plans
CREATE TABLE i3719 (time timestamptz NOT NULL,data text);
SELECT table_name FROM create_hypertable('i3719', 'time');
ALTER TABLE i3719 SET (timescaledb.compress);

INSERT INTO i3719 VALUES('2021-01-01 00:00:00', 'chunk 1');
SELECT count(compress_chunk(c)) FROM show_chunks('i3719') c;
INSERT INTO i3719 VALUES('2021-02-22 08:00:00', 'chunk 2');

SET plan_cache_mode TO force_generic_plan;
PREPARE p1(timestamptz) AS UPDATE i3719 SET data = 'x' WHERE time=$1;
PREPARE p2(timestamptz) AS DELETE FROM i3719 WHERE time=$1;
EXECUTE p1('2021-02-22T08:00:00+00');
EXECUTE p2('2021-02-22T08:00:00+00');

DEALLOCATE p1;
DEALLOCATE p2;

DROP TABLE i3719;

-- github issue 4778
CREATE TABLE metric_5m (
    time TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    series_id BIGINT NOT NULL
);
SELECT table_name FROM create_hypertable(
                            'metric_5m'::regclass,
                            'time'::name, chunk_time_interval=>interval '5m',
                            create_default_indexes=> false);
-- enable compression
ALTER TABLE metric_5m SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'series_id',
    timescaledb.compress_orderby = 'time, value'
);
SET work_mem TO '64kB';
SELECT '2022-10-10 14:33:44.1234+05:30' as start_date \gset
-- populate hypertable
INSERT INTO metric_5m (time, series_id, value)
    SELECT t, s,1 from generate_series(:'start_date'::timestamptz, :'start_date'::timestamptz + interval '1 day', '10s') t cross join generate_series(1,10, 1) s;
-- manually compress all chunks
SELECT count(compress_chunk(c)) FROM show_chunks('metric_5m') c;

-- populate into compressed hypertable, this should not crash
INSERT INTO metric_5m (time, series_id, value)
    SELECT t, s,1 from generate_series(:'start_date'::timestamptz, :'start_date'::timestamptz + interval '1 day', '10s') t cross join generate_series(1,10, 1) s;
-- clean up
RESET work_mem;
DROP TABLE metric_5m;

-- github issue 5134
CREATE TABLE mytab (time TIMESTAMPTZ NOT NULL, a INT, b INT, c INT);
SELECT table_name FROM create_hypertable('mytab', 'time', chunk_time_interval => interval '1 day');

INSERT INTO mytab
    SELECT time,
        CASE WHEN (:'start_date'::timestamptz - time < interval '1 days') THEN 1
             WHEN (:'start_date'::timestamptz - time < interval '2 days') THEN 2
             WHEN (:'start_date'::timestamptz - time < interval '3 days') THEN 3 ELSE 4 END as a
    from generate_series(:'start_date'::timestamptz - interval '3 days', :'start_date'::timestamptz, interval '5 sec') as g1(time);

-- enable compression
ALTER TABLE mytab SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'a, c'
);

-- get first chunk name
SELECT chunk_schema || '.' || chunk_name as "chunk_table"
       FROM timescaledb_information.chunks
       WHERE hypertable_name = 'mytab' ORDER BY range_start limit 1 \gset

-- compress only the first chunk
SELECT count(compress_chunk(:'chunk_table'));

-- insert a row into first compressed chunk
INSERT INTO mytab SELECT '2022-10-07 05:30:10+05:30'::timestamp with time zone, 3, 3;
-- should not crash
EXPLAIN (costs off) SELECT * FROM :chunk_table;
DROP TABLE mytab CASCADE;

-- test varchar segmentby
CREATE TABLE comp_seg_varchar (
  time timestamptz NOT NULL,
  source_id varchar(64) NOT NULL,
  label varchar NOT NULL,
  data jsonb
);

SELECT table_name FROM create_hypertable('comp_seg_varchar', 'time');

CREATE UNIQUE INDEX ON comp_seg_varchar(source_id, label, "time" DESC);

ALTER TABLE comp_seg_varchar SET(timescaledb.compress, timescaledb.compress_segmentby = 'source_id, label', timescaledb.compress_orderby = 'time');

INSERT INTO comp_seg_varchar
SELECT time, source_id, label, '{}' AS data
FROM
generate_series('1990-01-01'::timestamptz, '1990-01-10'::timestamptz, INTERVAL '1 day') AS g1(time),
generate_series(1, 3, 1 ) AS g2(source_id),
generate_series(1, 3, 1 ) AS g3(label);

SELECT count(compress_chunk(c)) FROM show_chunks('comp_seg_varchar') c;


-- all tuples should come from compressed chunks
EXPLAIN (analyze,costs off, timing off, summary off) SELECT * FROM comp_seg_varchar;

INSERT INTO comp_seg_varchar(time, source_id, label, data) VALUES ('1990-01-02 00:00:00+00', 'test', 'test', '{}'::jsonb)
ON CONFLICT (source_id, label, time) DO UPDATE SET data = '{"update": true}';

-- no tuples should be moved into uncompressed
EXPLAIN (analyze,costs off, timing off, summary off) SELECT * FROM comp_seg_varchar;

INSERT INTO comp_seg_varchar(time, source_id, label, data) VALUES ('1990-01-02 00:00:00+00', '1', '2', '{}'::jsonb)
ON CONFLICT (source_id, label, time) DO UPDATE SET data = '{"update": true}';

-- 1 batch should be moved into uncompressed
EXPLAIN (analyze,costs off, timing off, summary off) SELECT * FROM comp_seg_varchar;

DROP TABLE comp_seg_varchar;

-- test row locks for compressed tuples are blocked
CREATE TABLE row_locks(time timestamptz NOT NULL);
SELECT table_name FROM create_hypertable('row_locks', 'time');
ALTER TABLE row_locks SET (timescaledb.compress);
INSERT INTO row_locks VALUES('2021-01-01 00:00:00');
SELECT count(compress_chunk(c)) FROM show_chunks('row_locks') c;

-- should succeed cause no compressed tuples are returned
SELECT FROM row_locks WHERE time < '2021-01-01 00:00:00' FOR UPDATE;
-- should be blocked
\set ON_ERROR_STOP 0
SELECT FROM row_locks FOR UPDATE;
SELECT FROM row_locks FOR NO KEY UPDATE;
SELECT FROM row_locks FOR SHARE;
SELECT FROM row_locks FOR KEY SHARE;
\set ON_ERROR_STOP 1

DROP TABLE row_locks;

CREATE TABLE lazy_decompress(time timestamptz not null, device text, value float, primary key (device,time));
SELECT table_name FROM create_hypertable('lazy_decompress', 'time');
ALTER TABLE lazy_decompress SET (timescaledb.compress, timescaledb.compress_segmentby = 'device');

INSERT INTO lazy_decompress SELECT '2024-01-01'::timestamptz + format('%s',i)::interval, 'd1', i FROM generate_series(1,6000) g(i);

SELECT count(compress_chunk(c)) FROM show_chunks('lazy_decompress') c;

-- no decompression cause no match in batch
BEGIN; :ANALYZE INSERT INTO lazy_decompress SELECT '2024-01-01 0:00:00.5','d1',random() ON CONFLICT DO NOTHING; ROLLBACK;
BEGIN; :ANALYZE INSERT INTO lazy_decompress SELECT '2024-01-01 0:00:00.5','d1',random() ON CONFLICT(time,device) DO UPDATE SET value=EXCLUDED.value; ROLLBACK;
-- should decompress 1 batch cause there is match
BEGIN; :ANALYZE INSERT INTO lazy_decompress SELECT '2024-01-01 0:00:01','d1',random() ON CONFLICT DO NOTHING; ROLLBACK;
