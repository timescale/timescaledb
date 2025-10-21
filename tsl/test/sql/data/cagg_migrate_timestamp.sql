-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE public.conditions (
    "time" timestamp without time zone NOT NULL,
    temperature numeric
);

CREATE VIEW _timescaledb_internal._direct_view_6 AS
 SELECT public.time_bucket('1 day'::interval, "time") AS bucket,
    min(temperature) AS min,
    max(temperature) AS max,
    avg(temperature) AS avg,
    sum(temperature) AS sum
   FROM public.conditions
  GROUP BY (public.time_bucket('1 day'::interval, "time"));

CREATE VIEW _timescaledb_internal._direct_view_7 AS
 SELECT public.time_bucket('1 day'::interval, "time") AS bucket,
    min(temperature) AS min,
    max(temperature) AS max,
    avg(temperature) AS avg,
    sum(temperature) AS sum
   FROM public.conditions
  GROUP BY (public.time_bucket('1 day'::interval, "time"));

CREATE VIEW _timescaledb_internal._direct_view_8 AS
 SELECT public.time_bucket('7 days'::interval, "time") AS bucket,
    min(temperature) AS min,
    max(temperature) AS max,
    avg(temperature) AS avg,
    sum(temperature) AS sum
   FROM public.conditions
  GROUP BY (public.time_bucket('7 days'::interval, "time"));

CREATE TABLE _timescaledb_internal._materialized_hypertable_6 (
    bucket timestamp without time zone NOT NULL,
    min numeric,
    max numeric,
    avg numeric,
    sum numeric
);

CREATE TABLE _timescaledb_internal._materialized_hypertable_7 (
    bucket timestamp without time zone NOT NULL,
    agg_2_2 bytea,
    agg_3_3 bytea,
    agg_4_4 bytea,
    agg_5_5 bytea,
    chunk_id integer
);

CREATE TABLE _timescaledb_internal._materialized_hypertable_8 (
    bucket timestamp without time zone NOT NULL,
    agg_2_2 bytea,
    agg_3_3 bytea,
    agg_4_4 bytea,
    agg_5_5 bytea,
    chunk_id integer
);

CREATE VIEW _timescaledb_internal._partial_view_6 AS
 SELECT public.time_bucket('1 day'::interval, "time") AS bucket,
    min(temperature) AS min,
    max(temperature) AS max,
    avg(temperature) AS avg,
    sum(temperature) AS sum
   FROM public.conditions
  GROUP BY (public.time_bucket('1 day'::interval, "time"));

CREATE VIEW _timescaledb_internal._partial_view_7 AS
 SELECT public.time_bucket('1 day'::interval, "time") AS bucket,
    _timescaledb_functions.partialize_agg(min(temperature)) AS agg_2_2,
    _timescaledb_functions.partialize_agg(max(temperature)) AS agg_3_3,
    _timescaledb_functions.partialize_agg(avg(temperature)) AS agg_4_4,
    _timescaledb_functions.partialize_agg(sum(temperature)) AS agg_5_5,
    _timescaledb_functions.chunk_id_from_relid(tableoid) AS chunk_id
   FROM public.conditions
  GROUP BY (public.time_bucket('1 day'::interval, "time")), (_timescaledb_functions.chunk_id_from_relid(tableoid));

CREATE VIEW _timescaledb_internal._partial_view_8 AS
 SELECT public.time_bucket('7 days'::interval, "time") AS bucket,
    _timescaledb_functions.partialize_agg(min(temperature)) AS agg_2_2,
    _timescaledb_functions.partialize_agg(max(temperature)) AS agg_3_3,
    _timescaledb_functions.partialize_agg(avg(temperature)) AS agg_4_4,
    _timescaledb_functions.partialize_agg(sum(temperature)) AS agg_5_5,
    _timescaledb_functions.chunk_id_from_relid(tableoid) AS chunk_id
   FROM public.conditions
  GROUP BY (public.time_bucket('7 days'::interval, "time")), (_timescaledb_functions.chunk_id_from_relid(tableoid));

CREATE VIEW public.conditions_summary_daily AS
 SELECT _materialized_hypertable_7.bucket,
    _timescaledb_functions.finalize_agg('pg_catalog.min(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_7.agg_2_2, NULL::numeric) AS min,
    _timescaledb_functions.finalize_agg('pg_catalog.max(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_7.agg_3_3, NULL::numeric) AS max,
    _timescaledb_functions.finalize_agg('pg_catalog.avg(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_7.agg_4_4, NULL::numeric) AS avg,
    _timescaledb_functions.finalize_agg('pg_catalog.sum(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_7.agg_5_5, NULL::numeric) AS sum
   FROM _timescaledb_internal._materialized_hypertable_7
  WHERE (_materialized_hypertable_7.bucket < COALESCE(_timescaledb_functions.to_timestamp_without_timezone(_timescaledb_functions.cagg_watermark(7)), '-infinity'::timestamp without time zone))
  GROUP BY _materialized_hypertable_7.bucket
UNION ALL
 SELECT public.time_bucket('1 day'::interval, conditions."time") AS bucket,
    min(conditions.temperature) AS min,
    max(conditions.temperature) AS max,
    avg(conditions.temperature) AS avg,
    sum(conditions.temperature) AS sum
   FROM public.conditions
  WHERE (conditions."time" >= COALESCE(_timescaledb_functions.to_timestamp_without_timezone(_timescaledb_functions.cagg_watermark(7)), '-infinity'::timestamp without time zone))
  GROUP BY (public.time_bucket('1 day'::interval, conditions."time"));

CREATE VIEW public.conditions_summary_daily_new AS
 SELECT _materialized_hypertable_6.bucket,
    _materialized_hypertable_6.min,
    _materialized_hypertable_6.max,
    _materialized_hypertable_6.avg,
    _materialized_hypertable_6.sum
   FROM _timescaledb_internal._materialized_hypertable_6
  WHERE (_materialized_hypertable_6.bucket < COALESCE(_timescaledb_functions.to_timestamp_without_timezone(_timescaledb_functions.cagg_watermark(6)), '-infinity'::timestamp without time zone))
UNION ALL
 SELECT public.time_bucket('1 day'::interval, conditions."time") AS bucket,
    min(conditions.temperature) AS min,
    max(conditions.temperature) AS max,
    avg(conditions.temperature) AS avg,
    sum(conditions.temperature) AS sum
   FROM public.conditions
  WHERE (conditions."time" >= COALESCE(_timescaledb_functions.to_timestamp_without_timezone(_timescaledb_functions.cagg_watermark(6)), '-infinity'::timestamp without time zone))
  GROUP BY (public.time_bucket('1 day'::interval, conditions."time"));

CREATE VIEW public.conditions_summary_weekly AS
 SELECT _materialized_hypertable_8.bucket,
    _timescaledb_functions.finalize_agg('pg_catalog.min(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_8.agg_2_2, NULL::numeric) AS min,
    _timescaledb_functions.finalize_agg('pg_catalog.max(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_8.agg_3_3, NULL::numeric) AS max,
    _timescaledb_functions.finalize_agg('pg_catalog.avg(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_8.agg_4_4, NULL::numeric) AS avg,
    _timescaledb_functions.finalize_agg('pg_catalog.sum(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_8.agg_5_5, NULL::numeric) AS sum
   FROM _timescaledb_internal._materialized_hypertable_8
  WHERE (_materialized_hypertable_8.bucket < COALESCE(_timescaledb_functions.to_timestamp_without_timezone(_timescaledb_functions.cagg_watermark(8)), '-infinity'::timestamp without time zone))
  GROUP BY _materialized_hypertable_8.bucket
UNION ALL
 SELECT public.time_bucket('7 days'::interval, conditions."time") AS bucket,
    min(conditions.temperature) AS min,
    max(conditions.temperature) AS max,
    avg(conditions.temperature) AS avg,
    sum(conditions.temperature) AS sum
   FROM public.conditions
  WHERE (conditions."time" >= COALESCE(_timescaledb_functions.to_timestamp_without_timezone(_timescaledb_functions.cagg_watermark(8)), '-infinity'::timestamp without time zone))
  GROUP BY (public.time_bucket('7 days'::interval, conditions."time"));

COPY _timescaledb_catalog.hypertable (id, schema_name, table_name, associated_schema_name, associated_table_prefix, num_dimensions, chunk_sizing_func_schema, chunk_sizing_func_name, chunk_target_size, compression_state, compressed_hypertable_id, status) FROM stdin;
5	public	conditions	_timescaledb_internal	_hyper_5	1	_timescaledb_functions	calculate_chunk_interval	0	0	\N	0
6	_timescaledb_internal	_materialized_hypertable_6	_timescaledb_internal	_hyper_6	1	_timescaledb_functions	calculate_chunk_interval	0	0	\N	0
7	_timescaledb_internal	_materialized_hypertable_7	_timescaledb_internal	_hyper_7	1	_timescaledb_functions	calculate_chunk_interval	0	0	\N	0
8	_timescaledb_internal	_materialized_hypertable_8	_timescaledb_internal	_hyper_8	1	_timescaledb_functions	calculate_chunk_interval	0	0	\N	0
\.

COPY _timescaledb_catalog.dimension (id, hypertable_id, column_name, column_type, aligned, num_slices, partitioning_func_schema, partitioning_func, interval_length, compress_interval_length, integer_now_func_schema, integer_now_func) FROM stdin;
5	5	time	timestamp without time zone	t	\N	\N	\N	604800000000	\N	\N	\N
6	6	bucket	timestamp without time zone	t	\N	\N	\N	6048000000000	\N	\N	\N
7	7	bucket	timestamp without time zone	t	\N	\N	\N	6048000000000	\N	\N	\N
8	8	bucket	timestamp without time zone	t	\N	\N	\N	6048000000000	\N	\N	\N
\.

COPY _timescaledb_catalog.continuous_agg (mat_hypertable_id, raw_hypertable_id, parent_mat_hypertable_id, user_view_schema, user_view_name, partial_view_schema, partial_view_name, direct_view_schema, direct_view_name, materialized_only, finalized) FROM stdin;
6	5	\N	public	conditions_summary_daily_new	_timescaledb_internal	_partial_view_6	_timescaledb_internal	_direct_view_6	f	t
7	5	\N	public	conditions_summary_daily	_timescaledb_internal	_partial_view_7	_timescaledb_internal	_direct_view_7	f	f
8	5	\N	public	conditions_summary_weekly	_timescaledb_internal	_partial_view_8	_timescaledb_internal	_direct_view_8	f	f
\.

COPY _timescaledb_catalog.continuous_aggs_bucket_function (mat_hypertable_id, bucket_func, bucket_width, bucket_fixed_width) FROM stdin;
6	public.time_bucket(interval,timestamp without time zone)	1 day	f
7	public.time_bucket(interval,timestamp without time zone)	1 day	f
8	public.time_bucket(interval,timestamp without time zone)	10 days	f
\.

COPY _timescaledb_catalog.continuous_aggs_invalidation_threshold (hypertable_id, watermark) FROM stdin;
5	-210866803200000000
\.

COPY _timescaledb_catalog.continuous_aggs_materialization_invalidation_log (materialization_id, lowest_modified_value, greatest_modified_value) FROM stdin;
6	-9223372036854775808	9223372036854775807
7	-9223372036854775808	9223372036854775807
8	-9223372036854775808	9223372036854775807
\.

COPY _timescaledb_catalog.continuous_aggs_watermark (mat_hypertable_id, watermark) FROM stdin;
6	-210866803200000000
7	-210866803200000000
8	-210866803200000000
\.

SELECT pg_catalog.setval('_timescaledb_catalog.dimension_id_seq', 8, true);
SELECT pg_catalog.setval('_timescaledb_catalog.hypertable_id_seq', 8, true);

CREATE INDEX _materialized_hypertable_6_bucket_idx ON _timescaledb_internal._materialized_hypertable_6 USING btree (bucket DESC);

CREATE INDEX _materialized_hypertable_7_bucket_idx ON _timescaledb_internal._materialized_hypertable_7 USING btree (bucket DESC);

CREATE INDEX _materialized_hypertable_8_bucket_idx ON _timescaledb_internal._materialized_hypertable_8 USING btree (bucket DESC);

CREATE INDEX conditions_time_idx ON public.conditions USING btree ("time" DESC);

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON _timescaledb_internal._materialized_hypertable_6 FOR EACH ROW EXECUTE FUNCTION _timescaledb_functions.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON _timescaledb_internal._materialized_hypertable_7 FOR EACH ROW EXECUTE FUNCTION _timescaledb_functions.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON _timescaledb_internal._materialized_hypertable_8 FOR EACH ROW EXECUTE FUNCTION _timescaledb_functions.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON public.conditions FOR EACH ROW EXECUTE FUNCTION _timescaledb_functions.insert_blocker();

