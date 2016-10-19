DROP TYPE IF EXISTS ioql_return_partial CASCADE;
CREATE TYPE ioql_return_partial AS (
    group_field     TEXT,
    time            BIGINT,
    partial_value_1 DOUBLE PRECISION,
    partial_count   BIGINT
);

--TODO: change partial names so that they can't conflict with the group_field
CREATE OR REPLACE FUNCTION get_partial_aggregate_item_sql(item select_item)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN item.func = 'AVG' THEN
           format('sum(%1$s) as "%2$s_avg_sum", count(*) as "%2$s_avg_count"', get_field_value_specifier(item.field),
                  item.field)
       WHEN item.func = 'COUNT' THEN
           format('count(*) as "%2$s_count"', get_field_value_specifier(item.field), item.field)
       ELSE
           format('%3$s(%1$s) as "%2$s_%3$s"', get_field_value_specifier(item.field), item.field,
                  lower(item.func :: TEXT))
       END;
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION get_partial_aggregate_sql(items select_item [], agg aggregate_type)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN agg.group_field IS NOT NULL THEN
           format('%s, %s as group_time, %s', agg.group_field, get_time_clause(agg.group_time), field_list)
       ELSE
           format('%s as group_time, %s', get_time_clause(agg.group_time), field_list)
       END
FROM
    (SELECT array_to_string(ARRAY(SELECT get_partial_aggregate_item_sql(i)
                                  FROM unnest(items) i), ', ') AS field_list) AS field_sql;
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION get_combine_partial_aggregate_item_sql(item select_item)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN item.func = 'AVG' THEN
           format('sum("%1$s_avg_sum") as "%1$s_avg_sum", sum("%1$s_avg_count") as "%1$s_avg_count"', item.field)
       WHEN item.func = 'COUNT' THEN
           format('sum("%1$s_count") as "%1$s_count"', item.field)
       ELSE
           format('%2$s("%1$s_%2$s") as "%1$s_%2$s"', item.field, lower(item.func :: TEXT))
       END;
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION get_combine_partial_aggregate_sql(items select_item [], agg aggregate_type)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN agg.group_field IS NOT NULL THEN
           format('%s, group_time as group_time, %s', agg.group_field, field_list)
       ELSE
           format('group_time as group_time, %s', field_list)
       END
FROM
    (SELECT array_to_string(ARRAY(SELECT get_combine_partial_aggregate_item_sql(i)
                                  FROM unnest(items) i), ', ') AS field_list) AS field_sql;
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION get_combine_partial_aggregate_zero_value_item_sql(item select_item)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN item.func = 'AVG' THEN
           format('NULL::double precision as "%1$s_avg_sum", NULL::numeric as "%1$s_avg_count"', item.field)
       WHEN item.func = 'COUNT' THEN
           format('NULL::numeric as "%1$s_count"', item.field)
       ELSE
           format('NULL::double precision as "%1$s_%2$s"', item.field, lower(item.func :: TEXT))
       END;
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION get_combine_partial_aggregate_zero_value_sql(items select_item [], agg aggregate_type)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN agg.group_field IS NOT NULL THEN
           format('NULL::text as %s, NULL::bigint as group_time, %s', agg.group_field, field_list)
       ELSE
           format('NULL::bigint as group_time, %s', field_list)
       END
FROM
    (SELECT array_to_string(ARRAY(SELECT get_combine_partial_aggregate_zero_value_item_sql(i)
                                  FROM unnest(items) i), ', ') AS field_list) AS field_sql;
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION get_partial_aggregate_column_def_item_sql(item select_item)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN item.func = 'AVG' THEN
           format('"%1$s_avg_sum" double precision, "%1$s_avg_count" numeric', item.field)
       WHEN item.func = 'COUNT' THEN
           format('"%1$s_count" numeric', item.field)
       ELSE
           format('"%1$s_%2$s" double precision', item.field, lower(item.func :: TEXT))
       END;
$BODY$ LANGUAGE 'sql' IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION get_partial_aggregate_column_def(query ioql_query, items select_item [], agg aggregate_type)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN agg.group_field IS NOT NULL THEN
           format('%s, group_time bigint, %s', get_field_def(query.project_id, query.namespace_name, agg.group_field),
                  field_list)
       ELSE
           format('group_time bigint, %s', field_list)
       END
FROM
    (SELECT array_to_string(ARRAY(SELECT get_partial_aggregate_column_def_item_sql(i)
                                  FROM unnest(items) i), ', ') AS field_list) AS field_sql;
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION get_finalize_aggregate_item_sql(item select_item)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN item.func = 'AVG' THEN
           format('sum("%1$s_avg_sum")/sum("%1$s_avg_count") as "avg(%1$s)"', item.field)
       WHEN item.func = 'COUNT' THEN
           format('sum("%1$s_count") as "sum(%1$s)"', item.field)
       ELSE
           format('%2$s("%1$s_%2$s") as "%2$s(%1$s)"', item.field, lower(item.func :: TEXT))
       END;
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION get_finalize_aggregate_sql(items select_item [], agg aggregate_type)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN agg.group_field IS NOT NULL THEN
           format('%s, group_time as time, %s', agg.group_field, field_list)
       ELSE
           format('group_time as time, %s', field_list)
       END
FROM
    (SELECT array_to_string(ARRAY(SELECT get_finalize_aggregate_item_sql(i)
                                  FROM unnest(items) i), ', ') AS field_list) AS field_sql;
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION get_finalize_aggregate_column_def_item_sql(item select_item)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN item.func = 'AVG' THEN
           format('"avg(%s)" double precision', item.field)
       WHEN item.func = 'COUNT' THEN
           format('"count(%s)" numeric', item.field)
       ELSE
           format('"%2$s(%1$s)" double precision', item.field, lower(item.func :: TEXT))
       END;
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION get_finalize_aggregate_column_def(query ioql_query, items select_item [], agg aggregate_type)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN agg.group_field IS NOT NULL THEN
           format('%s, time bigint, %s', get_field_def(query.project_id, query.namespace_name, agg.group_field),
                  field_list)
       ELSE
           format('time bigint, %s', field_list)
       END
FROM
    (SELECT array_to_string(ARRAY(SELECT get_finalize_aggregate_column_def_item_sql(i)
                                  FROM unnest(items) i), ', ') AS field_list) AS field_sql;
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION base_query_agg_partial(query                  ioql_query, dt data_tables,
                                                  additional_constraints TEXT DEFAULT NULL)
    RETURNS TEXT AS
$BODY$
DECLARE
    ns            namespace_type;
    select_clause TEXT;
BEGIN
    ns := ROW (query.project_id, query.namespace_name, 1);
    select_clause :=
    'SELECT ' || get_partial_aggregate_sql(query.select_field, query.aggregate);
    RETURN base_query_raw(
        select_clause,
        format('FROM %s', dt.table_name),
        get_where_clause(
            default_predicates(ns, query, dt.total_partitions),
            additional_constraints,
            get_required_fields_predicate(dt.table_name :: TEXT, query)
        ),
        get_groupby_clause(query.aggregate),
        get_agg_orderby_clause(query), --need to order if will limit
        get_limit_clause(query.limit_rows + 1));
END
$BODY$
LANGUAGE PLPGSQL STABLE;

--------------- PARTITION FUNCTIONS ------------

CREATE OR REPLACE FUNCTION ioql_query_local_partition_agg_limit_rows_sql(
    query                  ioql_query,
    part                   namespace_partition_type,
    additional_constraints TEXT DEFAULT NULL)
    RETURNS TEXT AS $BODY$
SELECT format('SELECT * FROM (%s) as combined_partition LIMIT %L', string_agg(code_part, ' UNION ALL '
ORDER BY table_time DESC), query.limit_rows +
                           1) -- query.limit_rows + 1, needed since a group can span across time. +1 guarantees group was closed
FROM
    (SELECT
         '(' || code || ')' code_part,
         GREATEST(dt.start_time, dt.end_time) AS table_time
     FROM
             get_data_tables_for_partitions_time_desc(part) dt,
         LATERAL base_query_agg_partial(query, dt, additional_constraints) code) f;
$BODY$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION get_max_time_on_partition(part namespace_partition_type, add_constraint TEXT)
    RETURNS BIGINT AS $BODY$
DECLARE
    time           BIGINT;
    data_table_row data_tables;
BEGIN
    time := NULL;
    FOR data_table_row IN
    SELECT *
    FROM get_data_tables_for_partitions_time_desc(part) LOOP
        EXECUTE format(
            $$
         SELECT time
         FROM %s
         %s
         ORDER BY time DESC NULLS LAST
         LIMIT 1
      $$, data_table_row.table_name, get_where_clause(add_constraint))
        INTO time;
        IF time IS NOT NULL THEN
            RETURN time;
        END IF;
    END LOOP;
    RETURN NULL;
END
$BODY$ LANGUAGE PLPGSQL STABLE;

--------------- NODE FUNCTIONS ------------

CREATE OR REPLACE FUNCTION ioql_query_local_node_agg_ungrouped_sql(
    query                  ioql_query,
    additional_constraints TEXT DEFAULT NULL)
    RETURNS TEXT AS $BODY$
SELECT format('SELECT * FROM (%s) as combined_node LIMIT %L',
              coalesce(
                  string_agg(code_part, ' UNION ALL '),
                  format('SELECT %s WHERE FALSE',
                         get_combine_partial_aggregate_zero_value_sql(query.select_field, query.aggregate))
              ), query.limit_rows + 1)
-- query.limit_rows + 1, needed since a group can span across time. +1 guarantees group was closed
FROM
    (SELECT '(' || code || ')' code_part
     FROM get_partitions_for_namespace(get_namespace(query)) parts,
             ioql_query_local_partition_agg_limit_rows_sql(query, parts, additional_constraints) AS code) f;
$BODY$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION get_time_periods_limit(ns            namespace_type, additional_constraints TEXT,
                                                  period_length BIGINT, num_periods INT)
    RETURNS time_range LANGUAGE SQL STABLE AS
$BODY$
-- start and end inclusive
SELECT periods.*
FROM
(
    SELECT max(times) AS time
    FROM get_partitions_for_namespace(ns) parts,
            get_max_time_on_partition(parts, additional_constraints) times
) AS max_time,
        get_time_periods_limit_for_max(max_time.time, period_length, num_periods) AS periods
$BODY$;

CREATE OR REPLACE FUNCTION ioql_query_local_node_agg_grouped_sql(query                  ioql_query,
                                                                 additional_constraints TEXT DEFAULT NULL)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    trange        time_range;
    ns            namespace_type;
    ungrouped_sql TEXT;
    grouped_sql   TEXT;
BEGIN
    ns := get_namespace(query);
    IF query.limit_time_periods IS NOT NULL THEN
        trange := get_time_periods_limit(ns, combine_predicates(default_predicates(ns, query), additional_constraints),
                                         (query.aggregate).group_time, query.limit_time_periods);
        additional_constraints := combine_predicates(
            format('time >= %L AND time <=%L', trange.start_time, trange.end_time), additional_constraints);
    END IF;
    ungrouped_sql := ioql_query_local_node_agg_ungrouped_sql(query, additional_constraints);

    grouped_sql := format(
        $$
            SELECT
                %1$s
            FROM
                (%2$s) as ungrouped
            %3$s
            %4$s
            LIMIT %5$L
        $$,
        get_combine_partial_aggregate_sql(query.select_field, query.aggregate),
        ungrouped_sql,
        get_groupby_clause(query.aggregate),
        get_agg_orderby_clause(query),
        query.limit_rows);


    RAISE NOTICE E'Per-Node SQL:\n%\n', grouped_sql;

    RETURN grouped_sql;
END
$BODY$;


CREATE OR REPLACE FUNCTION ioql_query_local_node_agg(query ioql_query)
    RETURNS SETOF RECORD LANGUAGE PLPGSQL STABLE AS
$BODY$
BEGIN
    RETURN QUERY EXECUTE ioql_query_local_node_agg_grouped_sql(query);
END;
$BODY$;

--------------- CLUSTER FUNCTIONS ------------


CREATE OR REPLACE FUNCTION ioql_exec_query_agg_without_limit_sql(query ioql_query, ns namespace_type)
    RETURNS TEXT LANGUAGE SQL STABLE AS $BODY$
--function aggregates partials across nodes.
SELECT format(
    $$
          SELECT
            %1$s
          FROM  ioql_query_nodes_individually(
                  get_cluster_table(%4$L::namespace_type), %5$L, 'ioql_query_local_node_agg', %2$L
                ) as data(%2$s)
          %3$s
      $$,
    get_finalize_aggregate_sql(query.select_field, query.aggregate),
    get_partial_aggregate_column_def(query, query.select_field, query.aggregate),
    get_groupby_clause(query.aggregate),
    ns,
    query)
$BODY$;

CREATE OR REPLACE FUNCTION ioql_exec_query_agg(query ioql_query)
    RETURNS SETOF RECORD LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    ns  namespace_type;
    sql TEXT;
BEGIN
    ns := ROW (query.project_id, query.namespace_name, 1);
    IF query.limit_time_periods IS NOT NULL THEN
        sql := format(
            $$
                with without_limit AS (
                  %1$s
                )
                SELECT without_limit.*
                FROM  without_limit,
                      get_time_periods_limit_for_max((SELECT max(time) from without_limit), ($1.aggregate).group_time, $1.limit_time_periods) limits
                WHERE time >= limits.start_time AND time <= limits.end_time
                %2$s
                %3$s
            $$,
            ioql_exec_query_agg_without_limit_sql(query, ns),
            get_agg_orderby_clause(query, 'time'),
            get_limit_clause(query.limit_rows)
        );

        RAISE NOTICE E'Cross-node SQL:\n%\n', sql;
        RETURN QUERY EXECUTE sql
        USING query;
    ELSE
        sql := format(
            $$
                SELECT *
                FROM (%s) without_limit
                %s
                %s
            $$,
            ioql_exec_query_agg_without_limit_sql(query, ns),
            get_agg_orderby_clause(query, 'time'),
            get_limit_clause(query.limit_rows));

        RAISE NOTICE E'Cross-node SQL:\n%\n', sql;
        RETURN QUERY EXECUTE sql;
    END IF;
END;
$BODY$
SET constraint_exclusion = ON;



