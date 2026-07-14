-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\pset null '<NULL>'

SELECT * FROM _timescaledb_functions.get_hypertable_info('public.metrics'::regclass);
SELECT * FROM _timescaledb_functions.get_hypertable_info('public.hourly_device_metrics'::regclass);
SELECT * FROM _timescaledb_functions.get_hypertable_info(NULL);
SELECT * FROM _timescaledb_functions.get_hypertable_info(0);
SELECT * FROM _timescaledb_functions.get_hypertable_info('pg_catalog.pg_class'::regclass);

SELECT * FROM _timescaledb_functions.get_hypertable_info_by_id(1);
SELECT * FROM _timescaledb_functions.get_hypertable_info_by_id(6);
SELECT * FROM _timescaledb_functions.get_hypertable_info_by_id(0);
SELECT * FROM _timescaledb_functions.get_hypertable_info_by_id(NULL);

SELECT * FROM _timescaledb_functions.get_primary_dimension(1);
SELECT * FROM _timescaledb_functions.get_primary_dimension(6);
SELECT * FROM _timescaledb_functions.get_primary_dimension(0);
SELECT * FROM _timescaledb_functions.get_primary_dimension(NULL);

SELECT * FROM _timescaledb_functions.get_chunk_info('_timescaledb_internal._hyper_1_3_chunk');
SELECT * FROM _timescaledb_functions.get_chunk_info('_timescaledb_internal._hyper_1_3_chunk'::regclass);
SELECT * FROM _timescaledb_functions.get_chunk_info(0);
SELECT * FROM _timescaledb_functions.get_chunk_info(NULL);
SELECT * FROM _timescaledb_functions.get_chunk_info('pg_catalog.pg_class'::regclass);

SELECT * FROM _timescaledb_functions.get_chunk_info_by_id(3);
SELECT * FROM _timescaledb_functions.get_chunk_info_by_id(26);
SELECT * FROM _timescaledb_functions.get_chunk_info_by_id(0);
SELECT * FROM _timescaledb_functions.get_chunk_info_by_id(-1);
SELECT * FROM _timescaledb_functions.get_chunk_info_by_id(NULL);

SELECT * FROM _timescaledb_functions.get_chunk_primary_range('_timescaledb_internal._hyper_1_3_chunk'::regclass);
SELECT * FROM _timescaledb_functions.get_chunk_primary_range(0::regclass);
SELECT * FROM _timescaledb_functions.get_chunk_primary_range(1::regclass);
SELECT * FROM _timescaledb_functions.get_chunk_primary_range('pg_catalog.pg_class'::regclass);
SELECT * FROM _timescaledb_functions.get_chunk_primary_range(NULL::regclass);

SELECT * FROM _timescaledb_functions.get_chunk_primary_range_by_id(3);
SELECT * FROM _timescaledb_functions.get_chunk_primary_range_by_id(26);
SELECT * FROM _timescaledb_functions.get_chunk_primary_range_by_id(0::int);
SELECT * FROM _timescaledb_functions.get_chunk_primary_range_by_id(-1::int);
SELECT * FROM _timescaledb_functions.get_chunk_primary_range_by_id(NULL::int);

SELECT * FROM _timescaledb_functions.get_integer_now_func(1);
SELECT * FROM _timescaledb_functions.get_integer_now_func(6);
SELECT * FROM _timescaledb_functions.get_integer_now_func(0);
SELECT * FROM _timescaledb_functions.get_integer_now_func(-1);
SELECT * FROM _timescaledb_functions.get_integer_now_func(NULL);
