-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

CREATE TABLE public.conditions (
    "time" integer NOT NULL,
    temperature numeric
);

CREATE VIEW _timescaledb_internal._direct_view_2 AS
 SELECT public.time_bucket(24, "time") AS bucket,
    min(temperature) AS min,
    max(temperature) AS max,
    avg(temperature) AS avg,
    sum(temperature) AS sum
   FROM public.conditions
  GROUP BY (public.time_bucket(24, "time"));

CREATE VIEW _timescaledb_internal._direct_view_3 AS
 SELECT public.time_bucket(24, "time") AS bucket,
    min(temperature) AS min,
    max(temperature) AS max,
    avg(temperature) AS avg,
    sum(temperature) AS sum
   FROM public.conditions
  GROUP BY (public.time_bucket(24, "time"));

CREATE VIEW _timescaledb_internal._direct_view_4 AS
 SELECT public.time_bucket(168, "time") AS bucket,
    min(temperature) AS min,
    max(temperature) AS max,
    avg(temperature) AS avg,
    sum(temperature) AS sum
   FROM public.conditions
  GROUP BY (public.time_bucket(168, "time"));

CREATE TABLE _timescaledb_internal._materialized_hypertable_2 (
    bucket integer NOT NULL,
    min numeric,
    max numeric,
    avg numeric,
    sum numeric
);

CREATE TABLE _timescaledb_internal._materialized_hypertable_3 (
    bucket integer NOT NULL,
    agg_2_2 bytea,
    agg_3_3 bytea,
    agg_4_4 bytea,
    agg_5_5 bytea,
    chunk_id integer
);

CREATE TABLE _timescaledb_internal._materialized_hypertable_4 (
    bucket integer NOT NULL,
    agg_2_2 bytea,
    agg_3_3 bytea,
    agg_4_4 bytea,
    agg_5_5 bytea,
    chunk_id integer
);

CREATE VIEW _timescaledb_internal._partial_view_2 AS
 SELECT public.time_bucket(24, "time") AS bucket,
    min(temperature) AS min,
    max(temperature) AS max,
    avg(temperature) AS avg,
    sum(temperature) AS sum
   FROM public.conditions
  GROUP BY (public.time_bucket(24, "time"));

CREATE VIEW _timescaledb_internal._partial_view_3 AS
 SELECT public.time_bucket(24, "time") AS bucket,
    _timescaledb_functions.partialize_agg(min(temperature)) AS agg_2_2,
    _timescaledb_functions.partialize_agg(max(temperature)) AS agg_3_3,
    _timescaledb_functions.partialize_agg(avg(temperature)) AS agg_4_4,
    _timescaledb_functions.partialize_agg(sum(temperature)) AS agg_5_5,
    _timescaledb_functions.chunk_id_from_relid(tableoid) AS chunk_id
   FROM public.conditions
  GROUP BY (public.time_bucket(24, "time")), (_timescaledb_functions.chunk_id_from_relid(tableoid));

CREATE VIEW _timescaledb_internal._partial_view_4 AS
 SELECT public.time_bucket(168, "time") AS bucket,
    _timescaledb_functions.partialize_agg(min(temperature)) AS agg_2_2,
    _timescaledb_functions.partialize_agg(max(temperature)) AS agg_3_3,
    _timescaledb_functions.partialize_agg(avg(temperature)) AS agg_4_4,
    _timescaledb_functions.partialize_agg(sum(temperature)) AS agg_5_5,
    _timescaledb_functions.chunk_id_from_relid(tableoid) AS chunk_id
   FROM public.conditions
  GROUP BY (public.time_bucket(168, "time")), (_timescaledb_functions.chunk_id_from_relid(tableoid));

CREATE VIEW public.conditions_summary_daily AS
 SELECT _materialized_hypertable_3.bucket,
    _timescaledb_functions.finalize_agg('pg_catalog.min(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_3.agg_2_2, NULL::numeric) AS min,
    _timescaledb_functions.finalize_agg('pg_catalog.max(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_3.agg_3_3, NULL::numeric) AS max,
    _timescaledb_functions.finalize_agg('pg_catalog.avg(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_3.agg_4_4, NULL::numeric) AS avg,
    _timescaledb_functions.finalize_agg('pg_catalog.sum(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_3.agg_5_5, NULL::numeric) AS sum
   FROM _timescaledb_internal._materialized_hypertable_3
  WHERE (_materialized_hypertable_3.bucket < COALESCE((_timescaledb_functions.cagg_watermark(3))::integer, '-2147483648'::integer))
  GROUP BY _materialized_hypertable_3.bucket
UNION ALL
 SELECT public.time_bucket(24, conditions."time") AS bucket,
    min(conditions.temperature) AS min,
    max(conditions.temperature) AS max,
    avg(conditions.temperature) AS avg,
    sum(conditions.temperature) AS sum
   FROM public.conditions
  WHERE (conditions."time" >= COALESCE((_timescaledb_functions.cagg_watermark(3))::integer, '-2147483648'::integer))
  GROUP BY (public.time_bucket(24, conditions."time"));

CREATE VIEW public.conditions_summary_daily_new AS
 SELECT _materialized_hypertable_2.bucket,
    _materialized_hypertable_2.min,
    _materialized_hypertable_2.max,
    _materialized_hypertable_2.avg,
    _materialized_hypertable_2.sum
   FROM _timescaledb_internal._materialized_hypertable_2
  WHERE (_materialized_hypertable_2.bucket < COALESCE((_timescaledb_functions.cagg_watermark(2))::integer, '-2147483648'::integer))
UNION ALL
 SELECT public.time_bucket(24, conditions."time") AS bucket,
    min(conditions.temperature) AS min,
    max(conditions.temperature) AS max,
    avg(conditions.temperature) AS avg,
    sum(conditions.temperature) AS sum
   FROM public.conditions
  WHERE (conditions."time" >= COALESCE((_timescaledb_functions.cagg_watermark(2))::integer, '-2147483648'::integer))
  GROUP BY (public.time_bucket(24, conditions."time"));

CREATE VIEW public.conditions_summary_weekly AS
 SELECT _materialized_hypertable_4.bucket,
    _timescaledb_functions.finalize_agg('pg_catalog.min(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_4.agg_2_2, NULL::numeric) AS min,
    _timescaledb_functions.finalize_agg('pg_catalog.max(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_4.agg_3_3, NULL::numeric) AS max,
    _timescaledb_functions.finalize_agg('pg_catalog.avg(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_4.agg_4_4, NULL::numeric) AS avg,
    _timescaledb_functions.finalize_agg('pg_catalog.sum(numeric)'::text, NULL::name, NULL::name, '{{pg_catalog,numeric}}'::name[], _materialized_hypertable_4.agg_5_5, NULL::numeric) AS sum
   FROM _timescaledb_internal._materialized_hypertable_4
  WHERE (_materialized_hypertable_4.bucket < COALESCE((_timescaledb_functions.cagg_watermark(4))::integer, '-2147483648'::integer))
  GROUP BY _materialized_hypertable_4.bucket
UNION ALL
 SELECT public.time_bucket(168, conditions."time") AS bucket,
    min(conditions.temperature) AS min,
    max(conditions.temperature) AS max,
    avg(conditions.temperature) AS avg,
    sum(conditions.temperature) AS sum
   FROM public.conditions
  WHERE (conditions."time" >= COALESCE((_timescaledb_functions.cagg_watermark(4))::integer, '-2147483648'::integer))
  GROUP BY (public.time_bucket(168, conditions."time"));

COPY _timescaledb_catalog.hypertable (id, schema_name, table_name, associated_schema_name, associated_table_prefix, num_dimensions, chunk_sizing_func_schema, chunk_sizing_func_name, chunk_target_size, compression_state, compressed_hypertable_id, status) FROM stdin;
1	public	conditions	_timescaledb_internal	_hyper_1	1	_timescaledb_functions	calculate_chunk_interval	0	0	\N	0
2	_timescaledb_internal	_materialized_hypertable_2	_timescaledb_internal	_hyper_2	1	_timescaledb_functions	calculate_chunk_interval	0	0	\N	0
3	_timescaledb_internal	_materialized_hypertable_3	_timescaledb_internal	_hyper_3	1	_timescaledb_functions	calculate_chunk_interval	0	0	\N	0
4	_timescaledb_internal	_materialized_hypertable_4	_timescaledb_internal	_hyper_4	1	_timescaledb_functions	calculate_chunk_interval	0	0	\N	0
\.

COPY _timescaledb_catalog.dimension (id, hypertable_id, column_name, column_type, aligned, num_slices, partitioning_func_schema, partitioning_func, interval_length, compress_interval_length, integer_now_func_schema, integer_now_func) FROM stdin;
1	1	time	integer	t	\N	\N	\N	10	\N	public	integer_now
2	2	bucket	integer	t	\N	\N	\N	100	\N	\N	\N
3	3	bucket	integer	t	\N	\N	\N	100	\N	\N	\N
4	4	bucket	integer	t	\N	\N	\N	100	\N	\N	\N
\.

COPY _timescaledb_catalog.continuous_agg (mat_hypertable_id, raw_hypertable_id, parent_mat_hypertable_id, user_view_schema, user_view_name, partial_view_schema, partial_view_name, direct_view_schema, direct_view_name, materialized_only, finalized) FROM stdin;
2	1	\N	public	conditions_summary_daily_new	_timescaledb_internal	_partial_view_2	_timescaledb_internal	_direct_view_2	f	t
3	1	\N	public	conditions_summary_daily	_timescaledb_internal	_partial_view_3	_timescaledb_internal	_direct_view_3	f	f
4	1	\N	public	conditions_summary_weekly	_timescaledb_internal	_partial_view_4	_timescaledb_internal	_direct_view_4	f	f
\.

COPY _timescaledb_catalog.continuous_aggs_bucket_function (mat_hypertable_id, bucket_func, bucket_width, bucket_fixed_width) FROM stdin;
2	public.time_bucket(integer, integer)	24	t
3	public.time_bucket(integer, integer)	24	t
4	public.time_bucket(integer, integer)	168	t
\.

COPY _timescaledb_catalog.continuous_aggs_invalidation_threshold (hypertable_id, watermark) FROM stdin;
1	-2147483648
\.

COPY _timescaledb_catalog.continuous_aggs_materialization_invalidation_log (materialization_id, lowest_modified_value, greatest_modified_value) FROM stdin;
2	-9223372036854775808	9223372036854775807
3	-9223372036854775808	9223372036854775807
4	-9223372036854775808	9223372036854775807
\.

COPY _timescaledb_catalog.continuous_aggs_watermark (mat_hypertable_id, watermark) FROM stdin;
2	-2147483648
3	-2147483648
4	-2147483648
\.

SELECT pg_catalog.setval('_timescaledb_catalog.dimension_id_seq', 4, true);
SELECT pg_catalog.setval('_timescaledb_catalog.hypertable_id_seq', 4, true);

CREATE INDEX _materialized_hypertable_2_bucket_idx ON _timescaledb_internal._materialized_hypertable_2 USING btree (bucket DESC);

CREATE INDEX _materialized_hypertable_3_bucket_idx ON _timescaledb_internal._materialized_hypertable_3 USING btree (bucket DESC);

CREATE INDEX _materialized_hypertable_4_bucket_idx ON _timescaledb_internal._materialized_hypertable_4 USING btree (bucket DESC);

CREATE INDEX conditions_time_idx ON public.conditions USING btree ("time" DESC);

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON _timescaledb_internal._materialized_hypertable_2 FOR EACH ROW EXECUTE FUNCTION _timescaledb_functions.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON _timescaledb_internal._materialized_hypertable_3 FOR EACH ROW EXECUTE FUNCTION _timescaledb_functions.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON _timescaledb_internal._materialized_hypertable_4 FOR EACH ROW EXECUTE FUNCTION _timescaledb_functions.insert_blocker();

CREATE TRIGGER ts_insert_blocker BEFORE INSERT ON public.conditions FOR EACH ROW EXECUTE FUNCTION _timescaledb_functions.insert_blocker();

