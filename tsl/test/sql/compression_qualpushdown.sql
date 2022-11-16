-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- qual pushdown tests for decompresschunk ---
-- Test qual pushdown with ints
CREATE TABLE meta (device_id INT PRIMARY KEY);
CREATE TABLE hyper(
    time INT NOT NULL,
    device_id INT REFERENCES meta(device_id) ON DELETE CASCADE ON UPDATE CASCADE,
    val INT);
SELECT * FROM create_hypertable('hyper', 'time', chunk_time_interval => 10);
ALTER TABLE hyper SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'time',
    timescaledb.compress_segmentby = 'device_id');
INSERT INTO meta VALUES (1), (2), (3), (4), (5);
INSERT INTO hyper VALUES (1, 1, 1), (2, 2, 1), (3, 3, 1), (10, 3, 2), (11, 4, 2), (11, 5, 2);

SELECT ch1.table_name AS "CHUNK_NAME", ch1.schema_name|| '.' || ch1.table_name AS "CHUNK_FULL_NAME"
FROM _timescaledb_catalog.chunk ch1, _timescaledb_catalog.hypertable ht
WHERE ch1.hypertable_id = ht.id AND ht.table_name LIKE 'hyper'
ORDER BY ch1.id LIMIT 1 \gset

SELECT compress_chunk(:'CHUNK_FULL_NAME');

-- test for qual pushdown
explain (costs off, verbose)
SELECT
FROM hyper
WHERE time > 2::bigint and time < 4;

explain (costs off, verbose)
SELECT
FROM hyper
WHERE time = 3::bigint;

SELECT *
FROM hyper
WHERE time > 2::bigint and time < 4;

SELECT *
FROM hyper
WHERE time = 3::bigint;

--- github issue 1855
--- TESTs for meta column pushdown filters on exprs with casts.
CREATE TABLE metaseg_tab (
fmid smallint,
factorid smallint,
start_dt timestamp without time zone,
end_dt timestamp without time zone,
interval_number smallint,
logret double precision,
knowledge_date date NOT NULL
);
SELECT create_hypertable('metaseg_tab', 'end_dt', chunk_time_interval=>interval '1 month', create_default_indexes=>false);
SELECT add_dimension('metaseg_tab', 'fmid', chunk_time_interval => 1);
ALTER TABLE metaseg_tab SET (timescaledb.compress, timescaledb.compress_orderby= 'end_dt');
INSERT INTO metaseg_tab values (56,0,'2012-12-10 09:45:00','2012-12-10 09:50:00',1,0.1,'2012-12-10');

SELECT compress_chunk(i) from show_chunks('metaseg_tab') i;
select factorid, end_dt, logret
from metaseg_tab
where fmid = 56
and end_dt between '2012-12-10'::date and '2012-12-11'::date
order by factorid, end_dt;

explain (costs off, verbose)
select factorid, end_dt, logret
from metaseg_tab
where fmid = 56
and end_dt between '2012-12-10'::date and '2012-12-11'::date
order by factorid, end_dt;

--no pushdown here
select factorid, end_dt, logret
from metaseg_tab
where fmid = 56
and end_dt::date between '2012-12-10'::timestamp and '2012-12-11'::date
order by factorid, end_dt;

explain (costs off, verbose)
select factorid, end_dt, logret
from metaseg_tab
where fmid = 56
and end_dt::date between '2012-12-10'::timestamp and '2012-12-11'::date
order by factorid, end_dt;

--should fail
\set ON_ERROR_STOP 0
select factorid, end_dt, logret
from metaseg_tab
where fmid = 56
and end_dt::date = 10;

select factorid, end_dt, logret
from metaseg_tab
where fmid = 56
and end_dt::date = 'dec 2010'::date;

-- test casts between different char types are pushed down
CREATE TABLE pushdown_relabel(time timestamptz NOT NULL, dev_vc varchar(10), dev_c char(10));
SELECT table_name FROM create_hypertable('pushdown_relabel', 'time');
ALTER TABLE pushdown_relabel SET (timescaledb.compress, timescaledb.compress_segmentby='dev_vc,dev_c');

INSERT INTO pushdown_relabel SELECT '2000-01-01','varchar','char';
SELECT compress_chunk(i) from show_chunks('pushdown_relabel') i;

EXPLAIN (costs off) SELECT * FROM pushdown_relabel WHERE dev_vc = 'varchar';
EXPLAIN (costs off) SELECT * FROM pushdown_relabel WHERE dev_c = 'char';
EXPLAIN (costs off) SELECT * FROM pushdown_relabel WHERE dev_vc = 'varchar' AND dev_c = 'char';
EXPLAIN (costs off) SELECT * FROM pushdown_relabel WHERE dev_vc = 'varchar'::char(10) AND dev_c = 'char'::varchar;

-- test again with index scans
SET enable_seqscan TO false;
EXPLAIN (costs off) SELECT * FROM pushdown_relabel WHERE dev_vc = 'varchar';
EXPLAIN (costs off) SELECT * FROM pushdown_relabel WHERE dev_c = 'char';
EXPLAIN (costs off) SELECT * FROM pushdown_relabel WHERE dev_vc = 'varchar' AND dev_c = 'char';
EXPLAIN (costs off) SELECT * FROM pushdown_relabel WHERE dev_vc = 'varchar'::char(10) AND dev_c = 'char'::varchar;

