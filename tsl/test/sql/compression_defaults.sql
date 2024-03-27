-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER

-- statitics on
CREATE TABLE "public"."metrics" (
    "time" timestamp with time zone NOT NULL,
    "device_id" "text",
    "val" double precision
) WITH (autovacuum_enabled=0);

SELECT create_hypertable('public.metrics', 'time', create_default_indexes=>false);

insert into metrics SELECT t, 1, extract(epoch from t) from generate_series
        ( '2007-02-01'::timestamp
        , '2008-04-01'::timestamp
        , '1 day'::interval) t;

insert into metrics SELECT t, 2, extract(epoch from t) from generate_series
        ( '2009-02-01'::timestamp
        , '2010-04-01'::timestamp
        , '1 day'::interval) t;

ANALYZE metrics;

--use the best-scenario unique index
CREATE UNIQUE INDEX test_idx ON metrics(device_id, time);
SELECT _timescaledb_functions.get_segmentby_defaults('public.metrics');
SELECT _timescaledb_functions.get_orderby_defaults('public.metrics', ARRAY['device_id']);

ALTER TABLE metrics SET (timescaledb.compress = true);
SELECT * FROM _timescaledb_catalog.compression_settings;
ALTER TABLE metrics SET (timescaledb.compress = false);

ALTER TABLE metrics SET (timescaledb.compress = true, timescaledb.compress_segmentby = 'device_id');
SELECT * FROM _timescaledb_catalog.compression_settings;
ALTER TABLE metrics SET (timescaledb.compress = false);

--make sure all the GUC combinations work
SET timescaledb.compression_segmentby_default_function = '';
SET timescaledb.compression_orderby_default_function = '';
ALTER TABLE metrics SET (timescaledb.compress = true);
SELECT * FROM _timescaledb_catalog.compression_settings;
ALTER TABLE metrics SET (timescaledb.compress = false);

SET timescaledb.compression_segmentby_default_function   = '';
RESET timescaledb.compression_orderby_default_function;
ALTER TABLE metrics SET (timescaledb.compress = true);
SELECT * FROM _timescaledb_catalog.compression_settings;
ALTER TABLE metrics SET (timescaledb.compress = false);

RESET timescaledb.compression_segmentby_default_function;
SET timescaledb.compression_orderby_default_function = '';
ALTER TABLE metrics SET (timescaledb.compress = true);
SELECT * FROM _timescaledb_catalog.compression_settings;
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
SELECT * FROM _timescaledb_catalog.compression_settings;
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