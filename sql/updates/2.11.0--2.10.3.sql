DROP FUNCTION IF EXISTS _timescaledb_internal.get_approx_row_count(REGCLASS);

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.continuous_aggs_watermark;

DROP TABLE IF EXISTS _timescaledb_catalog.continuous_aggs_watermark;

DROP FUNCTION IF EXISTS _timescaledb_internal.cagg_watermark_materialized(hypertable_id INTEGER);
DROP FUNCTION _timescaledb_internal.recompress_chunk_segmentwise(REGCLASS, BOOLEAN);
DROP FUNCTION _timescaledb_internal.get_compressed_chunk_index_for_recompression(REGCLASS);

CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_is_finite(
    val      BIGINT
)
    RETURNS BOOLEAN LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS
$BODY$
    --end values of bigint reserved for infinite
    SELECT val > (-9223372036854775808)::bigint AND val < 9223372036854775807::bigint
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_slice_get_constraint_sql(
    dimension_slice_id  INTEGER
)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    dimension_slice_row _timescaledb_catalog.dimension_slice;
    dimension_row _timescaledb_catalog.dimension;
    dimension_def TEXT;
    dimtype REGTYPE;
    parts TEXT[];
BEGIN
    SELECT * INTO STRICT dimension_slice_row
    FROM _timescaledb_catalog.dimension_slice
    WHERE id = dimension_slice_id;

    SELECT * INTO STRICT dimension_row
    FROM _timescaledb_catalog.dimension
    WHERE id = dimension_slice_row.dimension_id;

    IF dimension_row.partitioning_func_schema IS NOT NULL AND
       dimension_row.partitioning_func IS NOT NULL THEN
        SELECT prorettype INTO STRICT dimtype
        FROM pg_catalog.pg_proc pro
        WHERE pro.oid = format('%I.%I', dimension_row.partitioning_func_schema, dimension_row.partitioning_func)::regproc::oid;

        dimension_def := format('%1$I.%2$I(%3$I)',
             dimension_row.partitioning_func_schema,
             dimension_row.partitioning_func,
             dimension_row.column_name);
    ELSE
        dimension_def := format('%1$I', dimension_row.column_name);
        dimtype := dimension_row.column_type;
    END IF;

    IF dimension_row.num_slices IS NOT NULL THEN

        IF  _timescaledb_internal.dimension_is_finite(dimension_slice_row.range_start) THEN
            parts = parts || format(' %1$s >= %2$L ', dimension_def, dimension_slice_row.range_start);
        END IF;

        IF _timescaledb_internal.dimension_is_finite(dimension_slice_row.range_end) THEN
            parts = parts || format(' %1$s < %2$L ', dimension_def, dimension_slice_row.range_end);
        END IF;

        IF array_length(parts, 1) = 0 THEN
            RETURN NULL;
        END IF;
        return array_to_string(parts, 'AND');
    ELSE
        -- only works with time for now
        IF _timescaledb_internal.time_literal_sql(dimension_slice_row.range_start, dimtype) =
           _timescaledb_internal.time_literal_sql(dimension_slice_row.range_end, dimtype) THEN
            RAISE 'time-based constraints have the same start and end values for column "%": %',
                    dimension_row.column_name,
                    _timescaledb_internal.time_literal_sql(dimension_slice_row.range_end, dimtype);
        END IF;

        parts = ARRAY[]::text[];

        IF _timescaledb_internal.dimension_is_finite(dimension_slice_row.range_start) THEN
            parts = parts || format(' %1$s >= %2$s ',
            dimension_def,
            _timescaledb_internal.time_literal_sql(dimension_slice_row.range_start, dimtype));
        END IF;

        IF _timescaledb_internal.dimension_is_finite(dimension_slice_row.range_end) THEN
            parts = parts || format(' %1$s < %2$s ',
            dimension_def,
            _timescaledb_internal.time_literal_sql(dimension_slice_row.range_end, dimtype));
        END IF;

        return array_to_string(parts, 'AND');
    END IF;
END
$BODY$ SET search_path TO pg_catalog, pg_temp;

ALTER FUNCTION _timescaledb_functions.hist_sfunc (state INTERNAL, val DOUBLE PRECISION, MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.hist_combinefunc(state1 INTERNAL, state2 INTERNAL) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.hist_serializefunc(INTERNAL) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.hist_deserializefunc(bytea, INTERNAL) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.hist_finalfunc(state INTERNAL, val DOUBLE PRECISION, MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER) SET SCHEMA _timescaledb_internal;

ALTER FUNCTION _timescaledb_functions.first_sfunc(internal, anyelement, "any") SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.first_combinefunc(internal, internal) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.last_sfunc(internal, anyelement, "any") SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.last_combinefunc(internal, internal) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.bookend_finalfunc(internal, anyelement, "any") SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.bookend_serializefunc(internal) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.bookend_deserializefunc(bytea, internal) SET SCHEMA _timescaledb_internal;

ALTER FUNCTION _timescaledb_functions.compressed_data_in(CSTRING) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.compressed_data_out(_timescaledb_internal.compressed_data) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.compressed_data_send(_timescaledb_internal.compressed_data) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.compressed_data_recv(internal) SET SCHEMA _timescaledb_internal;

ALTER FUNCTION _timescaledb_functions.rxid_in(cstring) SET SCHEMA _timescaledb_internal;
ALTER FUNCTION _timescaledb_functions.rxid_out(@extschema@.rxid) SET SCHEMA _timescaledb_internal;

DROP SCHEMA _timescaledb_functions;

CREATE FUNCTION _timescaledb_internal.is_main_table(
    table_oid regclass
)
    RETURNS bool LANGUAGE SQL STABLE AS
$BODY$
    SELECT EXISTS(SELECT 1 FROM _timescaledb_catalog.hypertable WHERE table_name = relname AND schema_name = nspname)
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = table_oid;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Check if given table is a hypertable's main table
CREATE FUNCTION _timescaledb_internal.is_main_table(
    schema_name NAME,
    table_name  NAME
)
    RETURNS BOOLEAN LANGUAGE SQL STABLE AS
$BODY$
     SELECT EXISTS(
         SELECT 1 FROM _timescaledb_catalog.hypertable h
         WHERE h.schema_name = is_main_table.schema_name AND
               h.table_name = is_main_table.table_name
     );
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Get a hypertable given its main table OID
CREATE FUNCTION _timescaledb_internal.hypertable_from_main_table(
    table_oid regclass
)
    RETURNS _timescaledb_catalog.hypertable LANGUAGE SQL STABLE AS
$BODY$
    SELECT h.*
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    INNER JOIN _timescaledb_catalog.hypertable h ON (h.table_name = c.relname AND h.schema_name = n.nspname)
    WHERE c.OID = table_oid;
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE FUNCTION _timescaledb_internal.main_table_from_hypertable(
    hypertable_id int
)
    RETURNS regclass LANGUAGE SQL STABLE AS
$BODY$
    SELECT format('%I.%I',h.schema_name, h.table_name)::regclass
    FROM _timescaledb_catalog.hypertable h
    WHERE id = hypertable_id;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Gets the sql code for representing the literal for the given time value (in the internal representation) as the column_type.
CREATE FUNCTION _timescaledb_internal.time_literal_sql(
    time_value      BIGINT,
    column_type     REGTYPE
)
    RETURNS text LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    ret text;
BEGIN
    IF time_value IS NULL THEN
        RETURN format('%L', NULL);
    END IF;
    CASE column_type
      WHEN 'BIGINT'::regtype, 'INTEGER'::regtype, 'SMALLINT'::regtype THEN
        RETURN format('%L', time_value); -- scale determined by user.
      WHEN 'TIMESTAMP'::regtype THEN
        --the time_value for timestamps w/o tz does not depend on local timezones. So perform at UTC.
        RETURN format('TIMESTAMP %1$L', timezone('UTC',_timescaledb_internal.to_timestamp(time_value))); -- microseconds
      WHEN 'TIMESTAMPTZ'::regtype THEN
        -- assume time_value is in microsec
        RETURN format('TIMESTAMPTZ %1$L', _timescaledb_internal.to_timestamp(time_value)); -- microseconds
      WHEN 'DATE'::regtype THEN
        RETURN format('%L', timezone('UTC',_timescaledb_internal.to_timestamp(time_value))::date);
      ELSE
         EXECUTE 'SELECT format(''%L'', $1::' || column_type::text || ')' into ret using time_value;
         RETURN ret;
    END CASE;
END
$BODY$ SET search_path TO pg_catalog, pg_temp;

ALTER TABLE _timescaledb_config.bgw_job
    ALTER COLUMN owner SET DEFAULT pg_catalog.quote_ident(current_role)::regrole;

-- Rebuild the _timescaledb_catalog.continuous_agg_migrate_plan_step definition
ALTER TABLE _timescaledb_catalog.continuous_agg_migrate_plan_step
    DROP CONSTRAINT continuous_agg_migrate_plan_step_mat_hypertable_id_fkey;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.continuous_agg_migrate_plan;
CREATE TABLE _timescaledb_catalog._tmp_continuous_agg_migrate_plan AS SELECT mat_hypertable_id, start_ts, end_ts FROM _timescaledb_catalog.continuous_agg_migrate_plan;
DROP TABLE _timescaledb_catalog.continuous_agg_migrate_plan;

CREATE TABLE _timescaledb_catalog.continuous_agg_migrate_plan (
  mat_hypertable_id integer NOT NULL,
  start_ts TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.now(),
  end_ts TIMESTAMPTZ,
  -- table constraints
  CONSTRAINT continuous_agg_migrate_plan_pkey PRIMARY KEY (mat_hypertable_id),
  CONSTRAINT continuous_agg_migrate_plan_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.continuous_agg (mat_hypertable_id)
);

INSERT INTO _timescaledb_catalog.continuous_agg_migrate_plan SELECT * FROM _timescaledb_catalog._tmp_continuous_agg_migrate_plan;
DROP TABLE _timescaledb_catalog._tmp_continuous_agg_migrate_plan;

ALTER TABLE _timescaledb_catalog.continuous_agg_migrate_plan_step
    ADD CONSTRAINT continuous_agg_migrate_plan_step_mat_hypertable_id_fkey FOREIGN KEY (mat_hypertable_id) REFERENCES _timescaledb_catalog.continuous_agg_migrate_plan (mat_hypertable_id) ON DELETE CASCADE;

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.continuous_agg_migrate_plan', '');

GRANT SELECT ON TABLE _timescaledb_catalog.continuous_agg_migrate_plan TO PUBLIC;

ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.telemetry_event;

DROP TABLE IF EXISTS _timescaledb_catalog.telemetry_event;
