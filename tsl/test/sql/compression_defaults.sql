-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

CREATE VIEW chunk_settings AS
SELECT hypertable, count(*) as chunks, segmentby, orderby, index
FROM timescaledb_information.chunk_compression_settings cs group by hypertable,segmentby,orderby, index ORDER BY 1,2,3,4,5;

-- statitics on
CREATE TABLE "public"."metrics" (
    "time" timestamp with time zone NOT NULL,
    "device_id" "text",
    "val" double precision
) WITH (autovacuum_enabled=0);

SELECT create_hypertable('public.metrics', 'time', create_default_indexes=>false);

insert into metrics SELECT t, 1, extract(epoch from t) from generate_series
        ( '2007-02-01'::timestamp
        , '2007-04-01'::timestamp
        , '1 day'::interval) t;

insert into metrics SELECT t, 2, extract(epoch from t) from generate_series
        ( '2007-06-01'::timestamp
        , '2007-10-01'::timestamp
        , '1 day'::interval) t;

ANALYZE metrics;

--use the best-scenario unique index
CREATE UNIQUE INDEX test_idx ON metrics(device_id, time);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');
SELECT _timescaledb_functions.get_orderby_defaults('public.metrics', ARRAY['device_id']);

ALTER TABLE metrics SET (timescaledb.compress = true);
select count(compress_chunk(x)) from show_chunks('metrics') x;
select * from chunk_settings;
select count(decompress_chunk(x)) from show_chunks('metrics') x;
ALTER TABLE metrics SET (timescaledb.compress = false);

ALTER TABLE metrics SET (timescaledb.compress = true, timescaledb.compress_segmentby = 'device_id');
select count(compress_chunk(x)) from show_chunks('metrics') x;
select * from chunk_settings;
select count(decompress_chunk(x)) from show_chunks('metrics') x;
ALTER TABLE metrics SET (timescaledb.compress = false);

--make sure all the GUC combinations work
SET timescaledb.compression_segmentby_default_function = '';
SET timescaledb.compression_orderby_default_function = '';
ALTER TABLE metrics SET (timescaledb.compress = true, timescaledb.compress_orderby = '"time" desc', timescaledb.compress_segmentby = '');
SELECT * FROM _timescaledb_catalog.compression_settings;
ALTER TABLE metrics SET (timescaledb.compress = false);

SET timescaledb.compression_segmentby_default_function   = '';
RESET timescaledb.compression_orderby_default_function;
ALTER TABLE metrics SET (timescaledb.compress = true);
select count(compress_chunk(x)) from show_chunks('metrics') x;
select * from chunk_settings;
select count(decompress_chunk(x)) from show_chunks('metrics') x;
ALTER TABLE metrics SET (timescaledb.compress = false);

RESET timescaledb.compression_segmentby_default_function;
SET timescaledb.compression_orderby_default_function = '';
ALTER TABLE metrics SET (timescaledb.compress = true);
select count(compress_chunk(x)) from show_chunks('metrics') x;
select * from chunk_settings;
select count(decompress_chunk(x)) from show_chunks('metrics') x;
ALTER TABLE metrics SET (timescaledb.compress = false);

RESET timescaledb.compression_segmentby_default_function;
RESET timescaledb.compression_orderby_default_function;

--opposite order of columns
drop index test_idx;
CREATE UNIQUE INDEX test_idx ON metrics(time, device_id);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');
SELECT _timescaledb_functions.get_orderby_defaults('public.metrics', ARRAY['device_id']);

--use a high-cardinality column in the index
drop index test_idx;
CREATE UNIQUE INDEX test_idx ON metrics(val, time);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');
SELECT _timescaledb_functions.get_orderby_defaults('public.metrics', ARRAY[]::text[]);

--use a non-unique index
drop index test_idx;
CREATE INDEX test_idx ON metrics(device_id, time, val);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');
SELECT _timescaledb_functions.get_orderby_defaults('public.metrics', ARRAY['device_id']::text[]);

--another non-unique index column order
drop index test_idx;
CREATE INDEX test_idx ON metrics(val, device_id, time);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');
SELECT _timescaledb_functions.get_orderby_defaults('public.metrics', ARRAY['device_id']::text[]);

--use a high-cardinality column in the non-unque index
drop index test_idx;
CREATE INDEX test_idx ON metrics(val, time);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');
SELECT _timescaledb_functions.get_orderby_defaults('public.metrics', ARRAY[]::text[]);

--use 2 indexes
drop index test_idx;
CREATE INDEX test_idx1 ON metrics(val, time);
CREATE INDEX test_idx2 ON metrics(device_id, time);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');
SELECT _timescaledb_functions.get_orderby_defaults('public.metrics', ARRAY['device_id']::text[]);

--no indexes
drop index test_idx1;
drop index test_idx2;
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');
SELECT _timescaledb_functions.get_orderby_defaults('public.metrics', ARRAY[]::text[]);

ALTER TABLE metrics SET (timescaledb.compress = true);
select count(compress_chunk(x)) from show_chunks('metrics') x;
select * from chunk_settings;
select count(decompress_chunk(x)) from show_chunks('metrics') x;
ALTER TABLE metrics SET (timescaledb.compress = false);

-- tables with no stats --
drop table metrics;

--serial case
CREATE TABLE "public"."metrics" (
     id serial NOT NULL,
    "time" timestamp with time zone NOT NULL,
    "device_id" "text",
    "val" double precision
) WITH (autovacuum_enabled=0);

SELECT create_hypertable('public.metrics', 'time', create_default_indexes=>false);

--no indexes
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');
SELECT _timescaledb_functions.get_orderby_defaults('public.metrics', ARRAY[]::text[]);

--minimum index
CREATE UNIQUE INDEX test_idx ON metrics(id, time);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');

--index with device_id
drop index test_idx;
CREATE UNIQUE INDEX test_idx ON metrics(device_id, id, time);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');

--two indexes
drop index test_idx;
CREATE UNIQUE INDEX test_idx ON metrics(id, time);
CREATE INDEX test_idx2 ON metrics(device_id, time);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');

--separate indexes
drop index test_idx;
drop index test_idx2;
CREATE INDEX test_idx ON metrics(id);
CREATE INDEX test_idx2 ON metrics(time);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');

--make sure generated identity also works
drop table metrics;
CREATE TABLE "public"."metrics" (
     id int GENERATED ALWAYS AS IDENTITY,
    "time" timestamp with time zone NOT NULL,
    "device_id" "text",
    "val" double precision
) WITH (autovacuum_enabled=0);

SELECT create_hypertable('public.metrics', 'time', create_default_indexes=>false);

--minimum index
CREATE UNIQUE INDEX test_idx ON metrics(id, time);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');

--index with device_id
drop index test_idx;
CREATE UNIQUE INDEX test_idx ON metrics(device_id, id, time);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');

--test default indexes
drop table metrics;
CREATE TABLE "public"."metrics" (
     id int GENERATED ALWAYS AS IDENTITY,
    "time" timestamp with time zone NOT NULL,
    "device_id" "text",
    "val" double precision
) WITH (autovacuum_enabled=0);

SELECT create_hypertable('public.metrics', 'time', create_default_indexes=>true);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');
SELECT _timescaledb_functions.get_orderby_defaults('public.metrics', ARRAY[]::text[]);

CREATE INDEX test_idx ON metrics(device_id);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');
SELECT _timescaledb_functions.get_orderby_defaults('public.metrics', ARRAY['device_id']);

drop index test_idx;
CREATE UNIQUE INDEX test_idx ON metrics(device_id, time);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');
SELECT _timescaledb_functions.get_orderby_defaults('public.metrics', ARRAY['device_id']::text[]);

--test table with skewed statistics
drop table metrics;
CREATE TABLE "public"."metrics" (
    "time" timestamp with time zone NOT NULL,
    "device_id" "text",
    "device_id2" "text",
    "val" double precision
) WITH (autovacuum_enabled=0);

SELECT create_hypertable('public.metrics', 'time', create_default_indexes=>false);

--skew device_id distribution compared to device_id2.
--device_id2 will be the favourable default segmentby
insert into metrics SELECT t, 1, 1, extract(epoch from t) from generate_series
        ( '2007-02-01'::timestamp
        , '2007-04-01'::timestamp
        , '1 day'::interval) t;

insert into metrics SELECT t, 1, 2, extract(epoch from t) from generate_series
        ( '2008-02-01'::timestamp
        , '2008-04-01'::timestamp
        , '1 day'::interval) t;

insert into metrics SELECT t, 1, 1, extract(epoch from t) from generate_series
        ( '2012-02-01'::timestamp
        , '2012-04-01'::timestamp
        , '1 day'::interval) t;

insert into metrics SELECT t, 2, 2, extract(epoch from t) from generate_series
        ( '2016-02-01'::timestamp
        , '2016-04-01'::timestamp
        , '1 day'::interval) t;

ANALYZE metrics;

--use the best-scenario unique index (device_id2)
CREATE UNIQUE INDEX test_idx ON metrics(device_id, time);
CREATE UNIQUE INDEX test_idx2 ON metrics(device_id2, time);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');

--opposite order of columns
drop index test_idx;
drop index test_idx2;
CREATE UNIQUE INDEX test_idx ON metrics(time, device_id);
CREATE UNIQUE INDEX test_idx2 ON metrics(time, device_id2);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');

--use a high-cardinality column in the index (still choose device_id2)
drop index test_idx;
CREATE UNIQUE INDEX test_idx ON metrics(val, time);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');

--use a non-unique index
drop index test_idx;
drop index test_idx2;
CREATE INDEX test_idx ON metrics(device_id, time, val);
CREATE INDEX test_idx2 ON metrics(device_id2, time, val);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');

--another non-unique index column order (choose device_id since it is in a lower index position)
drop index test_idx;
drop index test_idx2;
CREATE INDEX test_idx ON metrics(device_id, time, val);
CREATE INDEX test_idx2 ON metrics(val, device_id2, time);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');

--use a high-cardinality column in the non-unque index (still choose device_id2)
drop index test_idx;
CREATE INDEX test_idx ON metrics(val, time);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');

--use 2 indexes (choose device_id since it is an indexed column)
drop index test_idx;
drop index test_idx2;
CREATE INDEX test_idx ON metrics(val, time);
CREATE INDEX test_idx2 ON metrics(device_id, time);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');

--no indexes (choose device_id2)
drop index test_idx;
drop index test_idx2;
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');

ALTER TABLE metrics SET (timescaledb.compress = true);
select count(compress_chunk(x)) from show_chunks('metrics') x;
select * from chunk_settings;
select count(decompress_chunk(x)) from show_chunks('metrics') x;
ALTER TABLE metrics SET (timescaledb.compress = false);

ALTER TABLE metrics SET (timescaledb.compress = true, timescaledb.compress_segmentby = 'device_id');
select count(compress_chunk(x)) from show_chunks('metrics') x;
select * from chunk_settings;
select count(decompress_chunk(x)) from show_chunks('metrics') x;
ALTER TABLE metrics SET (timescaledb.compress = false);
DROP TABLE metrics;
--test on an empty order_by
CREATE TABLE table1(col1 INT NOT NULL, col2 INT);
SELECT create_hypertable('table1','col1', chunk_time_interval => 10);
SELECT _timescaledb_functions.get_orderby_defaults('table1', ARRAY['col1']::text[]);
ALTER TABLE table1 SET (timescaledb.compress, timescaledb.compress_segmentby = 'col1');
SELECT * FROM _timescaledb_catalog.compression_settings;
ALTER TABLE table1 SET (timescaledb.compress = false);

\set ON_ERROR_STOP 0
SET timescaledb.compression_segmentby_default_function = 'function_does_not_exist';
SET timescaledb.compression_orderby_default_function = 'function_does_not_exist';
--wrong function signatures
SET timescaledb.compression_segmentby_default_function = '_timescaledb_functions.get_orderby_defaults';
SET timescaledb.compression_orderby_default_function = '_timescaledb_functions.get_segmentby_defaults';
\set ON_ERROR_STOP 1
SET timescaledb.compression_orderby_default_function = '_timescaledb_functions.get_orderby_defaults';
SET timescaledb.compression_segmentby_default_function = '_timescaledb_functions.get_segmentby_defaults';
RESET timescaledb.compression_segmentby_default_function;
RESET timescaledb.compression_orderby_default_function;

-- test search_path quoting
SET search_path TO '';
CREATE TABLE public.t1 (time timestamptz NOT NULL, segment_value text NOT NULL);
SELECT public.create_hypertable('public.t1', 'time');
ALTER TABLE public.t1 SET (timescaledb.compress, timescaledb.compress_orderby = 'segment_value');
RESET search_path;

-- test same named objects in different schemas with default orderbys/segmentbys
\c :TEST_DBNAME :ROLE_SUPERUSER

CREATE SCHEMA schema1;
CREATE SCHEMA schema2;
GRANT ALL ON SCHEMA schema1, schema2 to public;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- test the case with default orderbys
CREATE TABLE schema1.page_events2 (
 time        TIMESTAMPTZ         NOT NULL,
 page        TEXT              NOT NULL
);

SELECT create_hypertable('schema1.page_events2', 'time');
ALTER TABLE schema1.page_events2 SET (
 timescaledb.compress,
 timescaledb.compress_segmentby = 'page'
);

CREATE TABLE schema2.page_events2 (
 time        TIMESTAMPTZ         NOT NULL,
 page        TEXT              NOT NULL
);

SELECT create_hypertable('schema2.page_events2', 'time');
ALTER TABLE schema2.page_events2 SET (
 timescaledb.compress,
 timescaledb.compress_segmentby = 'page'
);
DROP TABLE schema1.page_events2;
DROP TABLE schema2.page_events2;

-- test the case with default segmentbys
CREATE TABLE schema1.page_events2 (
 time        TIMESTAMPTZ         NOT NULL,
 id          BIGSERIAL           NOT NULL,
 page        TEXT                NOT NULL
);

SELECT create_hypertable('schema1.page_events2', 'time');
ALTER TABLE schema1.page_events2 SET (
 timescaledb.compress,
 timescaledb.compress_orderby = 'time desc, id asc'
);

CREATE TABLE schema2.page_events2 (
 time        TIMESTAMPTZ         NOT NULL UNIQUE,
 id          BIGSERIAL           NOT NULL,
 page        TEXT                NOT NULL
);

SELECT create_hypertable('schema2.page_events2', 'time');
ALTER TABLE schema2.page_events2 SET (
 timescaledb.compress,
 timescaledb.compress_orderby = 'time desc'
);

-- test default segmentby diabled when orderby is specified
CREATE TABLE test_table (
    ts   INT     NOT NULL,
    uuid BIGINT  NOT NULL,
    val  FLOAT
);
CREATE UNIQUE INDEX test_idx ON test_table (uuid, ts);

SELECT create_hypertable(
    'test_table',
    'ts',
    chunk_time_interval => 1500000
);

INSERT INTO test_table (ts, uuid, val) VALUES
  (1, 1001, 10.1),
  (2, 1002, 20.2),
  (3, 1003, 30.3);

SELECT _timescaledb_functions.get_segmentby_defaults('public.test_table');

ALTER TABLE test_table SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'uuid'
);
SELECT count(compress_chunk(x)) FROM show_chunks('test_table') x;
select * from chunk_settings;

DROP TABLE test_table;

-- test that date/time columns (category 'D') are not selected as segmentby
-- even when they appear in indexes
DROP TABLE IF EXISTS test_exclude_datetype;
CREATE TABLE test_exclude_datetype (
    ts            TIMESTAMPTZ     NOT NULL,
    event_date    DATE            NOT NULL,
    event_time    TIME            NOT NULL,
    device_id     TEXT            NOT NULL,
    sensor_id     INT             NOT NULL,
    value         FLOAT
) WITH (autovacuum_enabled=0, tsdb.hypertable);

INSERT INTO test_exclude_datetype
SELECT t, t::date, t::time, 'device_' || (i % 10), (i % 5), random() * 100
FROM generate_series('2025-01-01'::timestamptz, '2025-03-01'::timestamptz, '1 minute'::interval) WITH ORDINALITY AS g(t, i);

-- test deafults, should not select date and time columns
SELECT _timescaledb_functions.get_segmentby_defaults('public.test_exclude_datetype');

CREATE UNIQUE INDEX test_exclude_datetype_idx ON test_exclude_datetype(event_date, device_id, ts);
SELECT _timescaledb_functions.get_segmentby_defaults('public.test_exclude_datetype');

DROP INDEX test_exclude_datetype_idx;
CREATE UNIQUE INDEX test_exclude_datetype_idx ON test_exclude_datetype(event_time, device_id, ts);
SELECT _timescaledb_functions.get_segmentby_defaults('public.test_exclude_datetype');

DROP INDEX test_exclude_datetype_idx;
CREATE INDEX test_exclude_datetype_idx ON test_exclude_datetype(event_date, device_id, sensor_id);
SELECT _timescaledb_functions.get_segmentby_defaults('public.test_exclude_datetype');

DROP INDEX test_exclude_datetype_idx;
CREATE INDEX test_exclude_datetype_idx ON test_exclude_datetype(event_time, sensor_id);
SELECT _timescaledb_functions.get_segmentby_defaults('public.test_exclude_datetype');

DROP INDEX test_exclude_datetype_idx;
CREATE INDEX test_exclude_datetype_idx ON test_exclude_datetype(event_date, ts);
SELECT _timescaledb_functions.get_segmentby_defaults('public.test_exclude_datetype');

DROP INDEX test_exclude_datetype_idx;
CREATE UNIQUE INDEX test_exclude_datetype_idx ON test_exclude_datetype(device_id, event_date, ts);
SELECT _timescaledb_functions.get_segmentby_defaults('public.test_exclude_datetype');

-- after pg_stats
ANALYZE test_exclude_datetype;

SELECT _timescaledb_functions.get_segmentby_defaults('public.test_exclude_datetype');

DROP INDEX test_exclude_datetype_idx;
CREATE UNIQUE INDEX test_exclude_datetype_idx ON test_exclude_datetype(event_date, device_id, ts);
SELECT _timescaledb_functions.get_segmentby_defaults('public.test_exclude_datetype');

DROP INDEX test_exclude_datetype_idx;
CREATE UNIQUE INDEX test_exclude_datetype_idx ON test_exclude_datetype(event_time, device_id, ts);
SELECT _timescaledb_functions.get_segmentby_defaults('public.test_exclude_datetype');

DROP INDEX test_exclude_datetype_idx;
CREATE INDEX test_exclude_datetype_idx ON test_exclude_datetype(event_date, device_id, sensor_id);
SELECT _timescaledb_functions.get_segmentby_defaults('public.test_exclude_datetype');

DROP INDEX test_exclude_datetype_idx;
CREATE INDEX test_exclude_datetype_idx ON test_exclude_datetype(event_time, sensor_id);
SELECT _timescaledb_functions.get_segmentby_defaults('public.test_exclude_datetype');

DROP INDEX test_exclude_datetype_idx;
CREATE UNIQUE INDEX test_exclude_datetype_idx ON test_exclude_datetype(device_id, event_date, ts);
SELECT _timescaledb_functions.get_segmentby_defaults('public.test_exclude_datetype');

ALTER TABLE test_exclude_datetype SET (timescaledb.compress = true);
SELECT count(compress_chunk(x)) FROM show_chunks('test_exclude_datetype') x;
SELECT * FROM timescaledb_information.chunk_compression_settings WHERE hypertable = 'test_exclude_datetype'::regclass ORDER BY chunk LIMIT 1;

DROP TABLE test_exclude_datetype;
