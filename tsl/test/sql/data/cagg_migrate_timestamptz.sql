-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE public.conditions (
    "time" timestamp with time zone NOT NULL,
    temperature numeric
);

CREATE VIEW _timescaledb_internal._direct_view_10 AS
 SELECT public.time_bucket('1 day'::interval, "time") AS bucket,
    min(temperature) AS min,
    max(temperature) AS max,
    avg(temperature) AS avg,
    sum(temperature) AS sum
   FROM public.conditions
  GROUP BY (public.time_bucket('1 day'::interval, "time"));

CREATE VIEW _timescaledb_internal._direct_view_11 AS
 SELECT public.time_bucket('1 day'::interval, "time") AS bucket,
    min(temperature) AS min,
    max(temperature) AS max,
    avg(temperature) AS avg,
    sum(temperature) AS sum
   FROM public.conditions
  GROUP BY (public.time_bucket('1 day'::interval, "time"));

CREATE VIEW _timescaledb_internal._direct_view_12 AS
 SELECT public.time_bucket('7 days'::interval, "time") AS bucket,
    min(temperature) AS min,
    max(temperature) AS max,
    avg(temperature) AS avg,
    sum(temperature) AS sum
   FROM public.conditions
  GROUP BY (public.time_bucket('7 days'::interval, "time"));

CREATE TABLE _timescaledb_internal._materialized_hypertable_10 (
    bucket timestamp with time zone NOT NULL,
    min numeric,
    max numeric,
    avg numeric,
    sum numeric
);

CREATE TABLE _timescaledb_internal._materialized_hypertable_11 (
    bucket timestamp with time zone NOT NULL,
    agg_2_2 bytea,
    agg_3_3 bytea,
    agg_4_4 bytea,
    agg_5_5 bytea,
    chunk_id integer
);

CREATE TABLE _timescaledb_internal._materialized_hypertable_12 (
    bucket timestamp with time zone NOT NULL,
    agg_2_2 bytea,
    agg_3_3 bytea,
    agg_4_4 bytea,
    agg_5_5 bytea,
    chunk_id integer
);

CREATE VIEW _timescaledb_internal._partial_view_10 AS
 SELECT public.time_bucket('1 day'::interval, "time") AS bucket,
    min(temperature) AS min,
    max(temperature) AS max,
    avg(temperature) AS avg,
    sum(temperature) AS sum
   FROM public.conditions
  GROUP BY (public.time_bucket('1 day'::interval, "time"));

CREATE VIEW _timescaledb_internal._partial_view_11 AS
 SELECT public.time_bucket('1 day'::interval, "time") AS bucket,
    _timescaledb_functions.partialize_agg(min(temperature)) AS agg_2_2,
    _timescaledb_functions.partialize_agg(max(temperature)) AS agg_3_3,
    _timescaledb_functions.partialize_agg(avg(temperature)) AS agg_4_4,
    _timescaledb_functions.partialize_agg(sum(temperature)) AS agg_5_5,
    _timescaledb_functions.chunk_id_from_relid(tableoid) AS chunk_id
   FROM public.conditions
  GROUP BY (public.time_bucket('1 day'::interval, "time")), (_timescaledb_functions.chunk_id_from_relid(tableoid));

CREATE VIEW _timescaledb_internal._partial_view_12 AS
 SELECT public.time_bucket('7 days'::interval, "time") AS bucket,
    _timescaledb_functions.partialize_agg(min(temperature)) AS agg_2_2,
    _timescaledb_functions.partialize_agg(max(temperature)) AS agg_3_3,
    _timescaledb_functions.partialize_agg(avg(temperature)) AS agg_4_4,
    _timescaledb_functions.partialize_agg(sum(temperature)) AS agg_5_5,
    _timescaledb_functions.chunk_id_from_relid(tableoid) AS chunk_id
   FROM public.conditions
  GROUP BY (public.time_bucket('7 days'::interval, "time")), (_timescaledb_functions.chunk_id_from_relid(tableoid));

CREATE VIEW public.conditions_summary_daily AS
 SELECT _materialized_hypertable_11.bucket,
    _timescaledb_functions.finalize_agg('pg_catalog.min(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_11.agg_2_2, NULL::numeric) AS min,
    _timescaledb_functions.finalize_agg('pg_catalog.max(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_11.agg_3_3, NULL::numeric) AS max,
    _timescaledb_functions.finalize_agg('pg_catalog.avg(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_11.agg_4_4, NULL::numeric) AS avg,
    _timescaledb_functions.finalize_agg('pg_catalog.sum(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_11.agg_5_5, NULL::numeric) AS sum
   FROM _timescaledb_internal._materialized_hypertable_11
  WHERE (_materialized_hypertable_11.bucket < COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(11)), '-infinity'::timestamp with time zone))
  GROUP BY _materialized_hypertable_11.bucket
UNION ALL
 SELECT public.time_bucket('1 day'::interval, conditions."time") AS bucket,
    min(conditions.temperature) AS min,
    max(conditions.temperature) AS max,
    avg(conditions.temperature) AS avg,
    sum(conditions.temperature) AS sum
   FROM public.conditions
  WHERE (conditions."time" >= COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(11)), '-infinity'::timestamp with time zone))
  GROUP BY (public.time_bucket('1 day'::interval, conditions."time"));

CREATE VIEW public.conditions_summary_daily_new AS
 SELECT _materialized_hypertable_10.bucket,
    _materialized_hypertable_10.min,
    _materialized_hypertable_10.max,
    _materialized_hypertable_10.avg,
    _materialized_hypertable_10.sum
   FROM _timescaledb_internal._materialized_hypertable_10
  WHERE (_materialized_hypertable_10.bucket < COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(10)), '-infinity'::timestamp with time zone))
UNION ALL
 SELECT public.time_bucket('1 day'::interval, conditions."time") AS bucket,
    min(conditions.temperature) AS min,
    max(conditions.temperature) AS max,
    avg(conditions.temperature) AS avg,
    sum(conditions.temperature) AS sum
   FROM public.conditions
  WHERE (conditions."time" >= COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(10)), '-infinity'::timestamp with time zone))
  GROUP BY (public.time_bucket('1 day'::interval, conditions."time"));

CREATE VIEW public.conditions_summary_weekly AS
 SELECT _materialized_hypertable_12.bucket,
    _timescaledb_functions.finalize_agg('pg_catalog.min(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_12.agg_2_2, NULL::numeric) AS min,
    _timescaledb_functions.finalize_agg('pg_catalog.max(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_12.agg_3_3, NULL::numeric) AS max,
    _timescaledb_functions.finalize_agg('pg_catalog.avg(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_12.agg_4_4, NULL::numeric) AS avg,
    _timescaledb_functions.finalize_agg('pg_catalog.sum(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_12.agg_5_5, NULL::numeric) AS sum
   FROM _timescaledb_internal._materialized_hypertable_12
  WHERE (_materialized_hypertable_12.bucket < COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(12)), '-infinity'::timestamp with time zone))
  GROUP BY _materialized_hypertable_12.bucket
UNION ALL
 SELECT public.time_bucket('7 days'::interval, conditions."time") AS bucket,
    min(conditions.temperature) AS min,
    max(conditions.temperature) AS max,
    avg(conditions.temperature) AS avg,
    sum(conditions.temperature) AS sum
   FROM public.conditions
  WHERE (conditions."time" >= COALESCE(_timescaledb_functions.to_timestamp(_timescaledb_functions.cagg_watermark(12)), '-infinity'::timestamp with time zone))
  GROUP BY (public.time_bucket('7 days'::interval, conditions."time"));

COPY _timescaledb_catalog.hypertable (id, schema_name, table_name, associated_schema_name, associated_table_prefix, num_dimensions, chunk_sizing_func_schema, chunk_sizing_func_name, chunk_target_size, compression_state, compressed_hypertable_id, status) FROM stdin;
9	public	conditions	_timescaledb_internal	_hyper_9	1	_timescaledb_functions	calculate_chunk_interval	0	0	\N	0
10	_timescaledb_internal	_materialized_hypertable_10	_timescaledb_internal	_hyper_10	1	_timescaledb_functions	calculate_chunk_interval	0	0	\N	0
11	_timescaledb_internal	_materialized_hypertable_11	_timescaledb_internal	_hyper_11	1	_timescaledb_functions	calculate_chunk_interval	0	0	\N	0
12	_timescaledb_internal	_materialized_hypertable_12	_timescaledb_internal	_hyper_12	1	_timescaledb_functions	calculate_chunk_interval	0	0	\N	0
\.

COPY _timescaledb_catalog.dimension (id, hypertable_id, column_name, column_type, aligned, num_slices, partitioning_func_schema, partitioning_func, interval_length, compress_interval_length, integer_now_func_schema, integer_now_func) FROM stdin;
9	9	time	timestamp with time zone	t	\N	\N	\N	604800000000	\N	\N	\N
10	10	bucket	timestamp with time zone	t	\N	\N	\N	6048000000000	\N	\N	\N
11	11	bucket	timestamp with time zone	t	\N	\N	\N	6048000000000	\N	\N	\N
12	12	bucket	timestamp with time zone	t	\N	\N	\N	6048000000000	\N	\N	\N
\.

COPY _timescaledb_catalog.continuous_agg (mat_hypertable_id, raw_hypertable_id, parent_mat_hypertable_id, user_view_schema, user_view_name, partial_view_schema, partial_view_name, direct_view_schema, direct_view_name, materialized_only, finalized) FROM stdin;
10	9	\N	public	conditions_summary_daily_new	_timescaledb_internal	_partial_view_10	_timescaledb_internal	_direct_view_10	f	t
11	9	\N	public	conditions_summary_daily	_timescaledb_internal	_partial_view_11	_timescaledb_internal	_direct_view_11	f	f
12	9	\N	public	conditions_summary_weekly	_timescaledb_internal	_partial_view_12	_timescaledb_internal	_direct_view_12	f	f
\.

COPY _timescaledb_catalog.continuous_aggs_bucket_function (mat_hypertable_id, bucket_func, bucket_width, bucket_fixed_width) FROM stdin;
10	public.time_bucket(interval,timestamp with time zone)	1 day	f
11	public.time_bucket(interval,timestamp with time zone)	1 day	f
12	public.time_bucket(interval,timestamp with time zone)	10 days	f
\.

COPY _timescaledb_catalog.continuous_aggs_invalidation_threshold (hypertable_id, watermark) FROM stdin;
9	-210866803200000000
\.

COPY _timescaledb_catalog.continuous_aggs_materialization_invalidation_log (materialization_id, lowest_modified_value, greatest_modified_value) FROM stdin;
10	-9223372036854775808	9223372036854775807
11	-9223372036854775808	9223372036854775807
12	-9223372036854775808	9223372036854775807
\.

COPY _timescaledb_catalog.continuous_aggs_watermark (mat_hypertable_id, watermark) FROM stdin;
10	-210866803200000000
11	-210866803200000000
12	-210866803200000000
\.

SELECT pg_catalog.setval('_timescaledb_catalog.dimension_id_seq', 12, true);

CREATE INDEX _materialized_hypertable_10_bucket_idx ON _timescaledb_internal._materialized_hypertable_10 USING btree (bucket DESC);

CREATE INDEX _materialized_hypertable_11_bucket_idx ON _timescaledb_internal._materialized_hypertable_11 USING btree (bucket DESC);

CREATE INDEX _materialized_hypertable_12_bucket_idx ON _timescaledb_internal._materialized_hypertable_12 USING btree (bucket DESC);

CREATE INDEX conditions_time_idx ON public.conditions USING btree ("time" DESC);

