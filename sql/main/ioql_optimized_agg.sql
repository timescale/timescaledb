--TODO: change partial names so that they can't conflict with the group_field
CREATE OR REPLACE FUNCTION get_partial_aggregate_col_name(field_index BIGINT, field_name NAME, suffix NAME)
    RETURNS NAME AS $BODY$
SELECT format('%s_%s_%s', field_index, suffix, field_name) :: NAME;
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;

--TODO: change partial names so that they can't conflict with the group_field
CREATE OR REPLACE FUNCTION get_partial_aggregate_item_sql(field_index BIGINT, item select_item)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN item.func = 'AVG' THEN
           format('sum(%1$I) as %2$I, count(*) as %3$I',
                  item.field,
                  get_partial_aggregate_col_name(field_index, item.field, 'avg_sum'),
                  get_partial_aggregate_col_name(field_index, item.field, 'avg_count')
           )
       WHEN item.func = 'COUNT' THEN
           format('count(*) as %I',
                  get_partial_aggregate_col_name(field_index, item.field, 'count')
           )
       ELSE
           format('%2$s(%1$I) as %3$I',
                  item.field,
                  lower(item.func :: TEXT),
                  get_partial_aggregate_col_name(field_index, item.field, lower(item.func :: TEXT)))
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
    (
        SELECT array_to_string(
                   ARRAY(
                       SELECT get_partial_aggregate_item_sql(ord, ROW (field, func))
                       FROM unnest(items) WITH ORDINALITY AS x(field, func, ord)
                   )
                   , ', ') AS field_list
    ) AS field_sql;
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION get_combine_partial_aggregate_item_sql(field_index BIGINT, item select_item)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN item.func = 'AVG' THEN
           format('sum(%1$I) as %1$I, sum(%2$I) as %2$I',
                  get_partial_aggregate_col_name(field_index, item.field, 'avg_sum'),
                  get_partial_aggregate_col_name(field_index, item.field, 'avg_count')
           )
       WHEN item.func = 'COUNT' THEN
           format('sum(%1$I) as %1$I',
                  get_partial_aggregate_col_name(field_index, item.field, 'count')
           )
       ELSE
           format('%1$s(%2$I) as %2$I',
                  lower(item.func :: TEXT),
                  get_partial_aggregate_col_name(field_index, item.field,
                                                 lower(item.func :: TEXT))
           )
       END;
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION get_combine_partial_aggregate_sql(items select_item [], agg aggregate_type)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN agg.group_field IS NOT NULL THEN
           FORMAT('%s, group_time as group_time, %s', agg.group_field, field_list)
       ELSE
           format('group_time as group_time, %s', field_list)
       END
FROM
    (
        SELECT array_to_string(ARRAY(
                                   SELECT get_combine_partial_aggregate_item_sql(ord, ROW (field, func))
                                   FROM unnest(items) WITH ORDINALITY AS x(field, func, ord)
                               )
        , ', ') AS field_list
    ) AS field_sql;
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION get_combine_partial_aggregate_zero_value_item_sql(field_index BIGINT, item select_item)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN item.func = 'AVG' THEN
           format('NULL::double precision as %I, NULL::numeric as %I',
                  get_partial_aggregate_col_name(field_index, item.field, 'avg_sum'),
                  get_partial_aggregate_col_name(field_index, item.field, 'avg_count')
           )
       WHEN item.func = 'COUNT' THEN
           format('NULL::numeric as %I',
                  get_partial_aggregate_col_name(field_index, item.field, 'count')
           )
       ELSE
           format('NULL::double precision as %I',
                  get_partial_aggregate_col_name(field_index, item.field, lower(item.func :: TEXT))
           )
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
    (
        SELECT array_to_string(
                   ARRAY(
                       SELECT get_combine_partial_aggregate_zero_value_item_sql(ord, ROW (field, func))
                       FROM unnest(items) WITH ORDINALITY AS x(field, func, ord)
                   )
                   , ', ') AS field_list
    ) AS field_sql;
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION get_partial_aggregate_column_def_item_sql(field_index BIGINT, item select_item)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN item.func = 'AVG' THEN
           format('%I double precision, %I numeric',
                  get_partial_aggregate_col_name(field_index, item.field, 'avg_sum'),
                  get_partial_aggregate_col_name(field_index, item.field, 'avg_count')
           )
       WHEN item.func = 'COUNT' THEN
           format('%I numeric',
                  get_partial_aggregate_col_name(field_index, item.field, 'count'))
       ELSE
           format('%I double precision',
                  get_partial_aggregate_col_name(field_index, item.field, lower(item.func :: TEXT)))
       END;
$BODY$ LANGUAGE 'sql' IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION get_partial_aggregate_column_def(query ioql_query)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN (query.aggregate).group_field IS NOT NULL THEN
           format('%I %s, group_time bigint, %s', (query.aggregate).group_field,
                  get_field_type(query.namespace_name, (query.aggregate).group_field),
                  field_list)
       ELSE
           format('group_time bigint, %s', field_list)
       END
FROM
    (
        SELECT array_to_string(
                   ARRAY(
                       SELECT get_partial_aggregate_column_def_item_sql(ord, ROW (field, func))
                       FROM unnest(query.select_items) WITH ORDINALITY AS x(field, func, ord)
                   ), ', ') AS field_list
    ) AS field_sql;
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION get_finalize_aggregate_item_sql(field_index BIGINT, item select_item)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN item.func = 'AVG' THEN
           format('sum(%2$I)/sum(%3$I) AS %1$I',
                  get_result_aggregate_column_name(item.field, item.func),
                  get_partial_aggregate_col_name(field_index, item.field, 'avg_sum'),
                  get_partial_aggregate_col_name(field_index, item.field, 'avg_count')
           )
       WHEN item.func = 'COUNT' THEN
           format('sum(%2$I) AS %1$I',
                  get_result_aggregate_column_name(item.field, item.func),
                  get_partial_aggregate_col_name(field_index, item.field, 'count')
           )
       ELSE
           format('%1$s(%3$I) AS %2$I',
                  lower(item.func :: TEXT),
                  get_result_aggregate_column_name(item.field, item.func),
                  get_partial_aggregate_col_name(field_index, item.field, lower(item.func :: TEXT))
           )
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
    (
        SELECT array_to_string(
                   ARRAY(
                       SELECT get_finalize_aggregate_item_sql(ord, ROW (field, func))
                       FROM unnest(items) WITH ORDINALITY AS x(field, func, ord))
                   , ', ') AS field_list
    ) AS field_sql;
$BODY$ LANGUAGE SQL IMMUTABLE STRICT;


CREATE OR REPLACE FUNCTION base_query_agg_partial(query                  ioql_query, dt data_table,
                                                  additional_constraints TEXT, limit_sub TEXT,
                                                  include_table_time     BOOLEAN DEFAULT FALSE)
    RETURNS TEXT AS
$BODY$
DECLARE
    select_clause TEXT;
BEGIN
    IF include_table_time THEN
        select_clause :=
        format('SELECT %L::bigint as "_table_start", ', dt.start_time) ||
        get_partial_aggregate_sql(query.select_items, query.aggregate);
    ELSE
        select_clause :=
        'SELECT ' || get_partial_aggregate_sql(query.select_items, query.aggregate);
    END IF;

    RETURN base_query_raw(
        select_clause,
        format('FROM %s', dt.table_oid),
        get_where_clause(
            default_predicates(query, dt.total_partitions),
            additional_constraints
        ),
        get_groupby_clause(query.aggregate),
        get_orderby_clause_agg(query), --need to order if will limit
        'LIMIT ' || limit_sub
    );
END
$BODY$
LANGUAGE PLPGSQL STABLE;

--------------- PARTITION FUNCTIONS ------------


CREATE OR REPLACE FUNCTION ioql_query_local_partition_agg_limit_rows_sql(
    query                  ioql_query,
    part                   namespace_partition_type,
    additional_constraints TEXT DEFAULT NULL)
    RETURNS TEXT AS
$BODY$
DECLARE
    data_table_row  data_table;
    code            TEXT = '';
    index           INT = 0;
    previous_tables TEXT = '';
BEGIN
    FOR data_table_row IN SELECT *
                          FROM get_data_tables_for_partitions_time_desc(part) LOOP
        IF index = 0 THEN
            code := code || format($$  WITH results_%s AS (%s) $$,
                                   index,
                                   base_query_agg_partial(query, data_table_row, additional_constraints,
                                                          (query.limit_rows + 1) :: TEXT));
            previous_tables := 'SELECT * FROM results_0';
        ELSE
            --the stopping criteria is if I already have my limit+1 in distinct rows
            --otherwise have to try to get that many rows again
            --NOTE: Limiting by query.limit_rows - count(distinct) is not right
            --      this is because my new rows may overlap with previous rows.
            code := code || format($$  , results_%s AS (%s) $$, index,
                                   base_query_agg_partial(query, data_table_row, additional_constraints,
                                                          format(
                                                              '(SELECT CASE WHEN COUNT(DISTINCT group_time) = %1$L THEN 0 ELSE %1$L END FROM (%2$s) as x)',
                                                              query.limit_rows + 1,
                                                              previous_tables)
                                   ));
            previous_tables := previous_tables || format(' UNION ALL SELECT * FROM results_%s', index);
        END IF;
        index := index + 1;
    END LOOP;
    code := code || previous_tables;
    RETURN code;
END
$BODY$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION ioql_query_local_partition_agg_limit_rows_with_group_field_sql(
    query                  ioql_query,
    part                   namespace_partition_type,
    additional_constraints TEXT DEFAULT NULL)
    RETURNS TEXT AS
$BODY$
DECLARE
    data_table_row  data_table;
    code            TEXT = '';
    index           INT = 0;
    previous_tables TEXT = '';
    prev_start_time BIGINT;
BEGIN
    FOR data_table_row IN SELECT *
                          FROM get_data_tables_for_partitions_time_desc(part) LOOP
        IF index = 0 THEN
            --always scan first table
            code := code || format($$  WITH results_%s AS (%s) $$,
                                   index,
                                   base_query_agg_partial(query, data_table_row, additional_constraints,
                                                          (query.limit_rows) :: TEXT, TRUE));
            previous_tables := 'SELECT * FROM results_0';
        ELSE
            --continue going down unless the stopping criteria is met
            --stopping criteria is:
            -- 1) we already have limit_rows distinct group_field, time combinations
            -- 2) each of those top combinations belongs to a closed group.
            --    a closed group is one where the group starts after the start_time of the last table processed.
            code := code || format($$  , results_%s AS (%s) $$, index,
                                   base_query_agg_partial(query, data_table_row, additional_constraints,
                                                          format(
                                                              '(SELECT CASE WHEN COUNT(*) = %1$L AND min(group_time) > %4$L THEN 0 ELSE %1$L END '
                                                              ||
                                                              'FROM (SELECT DISTINCT %3$I, group_time FROM (%2$s) as y) as x)',
                                                              query.limit_rows,
                                                              previous_tables,
                                                              (query.aggregate).group_field,
                                                              prev_start_time
                                                          ),
                                                          TRUE
                                   ));

            --             code := code || format($$  , results_%s AS (%s) $$, index,
            --                                    base_query_agg_partial(
            --                                        query,
            --                                        data_table_row,
            --                                        additional_constraints,
            --                                        format(
            --                                            '(' ||
            --                                            'SELECT ' ||
            --                                            '  CASE WHEN COUNT(*) = %1$L AND  ' ||
            --                                            '  bool_and(_min_table_start IS NULL OR _min_table_start < group_time)' ||
            --                                            '  THEN ' ||
            --                                            '    0 ' ||
            --                                            ' ELSE ' ||
            --                                            '    %1$L ' ||
            --                                            'END ' ||
            --                                            'FROM (' ||
            --                                            '    SELECT ' ||
            --                                            '         %2$s, ' ||
            --                                            '         group_time, ' ||
            --                                            '         min(_table_start) as _min_table_start '
            --                                            '    FROM (%3$s) as x ' ||
            --                                            '    GROUP BY group_time, %2$s' ||
            --                                            '    ORDER BY group_time, %2$s' ||
            --                                            '    LIMIT %1$L' ||
            --                                            '      ) as agg' ||
            --                                            ')',
            --                                            query.limit_rows,
            --                                            (query.aggregate).group_field,
            --                                            previous_tables),
            --                                        TRUE
            --                                    ));
            previous_tables := previous_tables || format(' UNION ALL SELECT * FROM results_%s', index);
        END IF;
        prev_start_time := data_table_row.start_time;
        index := index + 1;
    END LOOP;
    code := code || previous_tables;
    RETURN code;
END
$BODY$ LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION ioql_query_local_partition_agg_sql(
    query                  ioql_query,
    part                   namespace_partition_type,
    additional_constraints TEXT DEFAULT NULL)
    RETURNS TEXT LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF (query.aggregate).group_field IS NULL THEN
        RETURN ioql_query_local_partition_agg_limit_rows_sql(query, part, additional_constraints);
    ELSE
        RETURN ioql_query_local_partition_agg_limit_rows_with_group_field_sql(query, part, additional_constraints);
    END IF;
END
$BODY$;


CREATE OR REPLACE FUNCTION get_max_time_on_partition(part namespace_partition_type, additional_constraints TEXT)
    RETURNS BIGINT AS
$BODY$
DECLARE
    time           BIGINT;
    data_table_row data_table;
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
            $$, data_table_row.table_oid, get_where_clause(additional_constraints))
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
SELECT format('SELECT * FROM (%s) as combined_node ',
              coalesce(
                  string_agg(code_part, ' UNION ALL '),
                  format('SELECT %s WHERE FALSE',
                         get_combine_partial_aggregate_zero_value_sql(query.select_items, query.aggregate))
              ))
-- query.limit_rows + 1, needed since a group can span across time. +1 guarantees group was closed
FROM
    (
        SELECT '(' || code || ')' code_part
        FROM get_partitions_for_namespace(query.namespace_name) parts,
            LATERAL ioql_query_local_partition_agg_sql(query, parts, additional_constraints) AS code
    ) AS f;
$BODY$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION get_time_periods_limit(namespace_name NAME, additional_constraints TEXT,
                                                  period_length  BIGINT, num_periods INT)
    RETURNS time_range LANGUAGE SQL STABLE AS
$BODY$
-- start and end inclusive
SELECT (get_time_periods_limit_for_max(max_time.max_time, period_length, num_periods)).*
FROM
    (
        SELECT max(get_max_time_on_partition(parts, additional_constraints)) AS max_time
        FROM get_partitions_for_namespace(namespace_name) parts
    ) AS max_time
$BODY$;

CREATE OR REPLACE FUNCTION ioql_query_local_node_agg_grouped_sql(query                  ioql_query,
                                                                 additional_constraints TEXT DEFAULT NULL)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    trange        time_range;
    ungrouped_sql TEXT;
    grouped_sql   TEXT;
BEGIN
    IF query.limit_time_periods IS NOT NULL THEN
        trange := get_time_periods_limit(query.namespace_name,
                                         combine_predicates(default_predicates(query), additional_constraints),
                                         (query.aggregate).group_time,
                                         query.limit_time_periods);
        additional_constraints := combine_predicates(
            format('time >= %L AND time <=%L', trange.start_time, trange.end_time),
            additional_constraints);
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
        get_combine_partial_aggregate_sql(query.select_items, query.aggregate),
        ungrouped_sql,
        get_groupby_clause(query.aggregate),
        get_orderby_clause_agg(query),
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


CREATE OR REPLACE FUNCTION ioql_exec_query_agg_without_limit_sql(query ioql_query)
    RETURNS TEXT LANGUAGE SQL STABLE AS $BODY$
--function aggregates partials across nodes.
SELECT format(
    $$
          SELECT
            %1$s
          FROM  ioql_query_nodes_individually(
                  get_cluster_table(%4$L), %5$L, 'ioql_query_local_node_agg', %2$L
                ) as data(%2$s)
          %3$s
      $$,
    get_finalize_aggregate_sql(query.select_items, query.aggregate),
    get_partial_aggregate_column_def(query),
    get_groupby_clause(query.aggregate),
    query.namespace_name,
    query)
$BODY$;

CREATE OR REPLACE FUNCTION ioql_exec_query_agg(query ioql_query)
    RETURNS SETOF RECORD LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    sql TEXT;
BEGIN
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
            ioql_exec_query_agg_without_limit_sql(query),
            get_orderby_clause_agg(query, 'time'),
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
            ioql_exec_query_agg_without_limit_sql(query),
            get_orderby_clause_agg(query, 'time'),
            get_limit_clause(query.limit_rows));

        RAISE NOTICE E'Cross-node SQL:\n%\n', sql;
        RETURN QUERY EXECUTE sql;
    END IF;
END;
$BODY$
SET constraint_exclusion = ON;



