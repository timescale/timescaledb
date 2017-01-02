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

CREATE OR REPLACE FUNCTION get_partial_aggregate_sql(time_col_name NAME, items select_item [], agg aggregate_type)
    RETURNS TEXT AS $BODY$
SELECT CASE
       WHEN agg.group_field IS NOT NULL THEN
           format('%s, %s as group_time, %s', agg.group_field, get_time_clause(time_col_name, agg.group_time), field_list)
       ELSE
           format('%s as group_time, %s', get_time_clause(time_col_name, agg.group_time), field_list)
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


CREATE OR REPLACE FUNCTION base_query_agg_partial(query                  ioql_query,
                                                  table_oid              REGCLASS,
                                                  epoch                  partition_epoch,
                                                  additional_constraints TEXT,
                                                  limit_sub              TEXT)
    RETURNS TEXT AS
$BODY$
DECLARE
    select_clause TEXT;
BEGIN

    select_clause :=
    'SELECT ' || get_partial_aggregate_sql(get_time_field(query.namespace_name), query.select_items, query.aggregate);

    RETURN base_query_raw(
        select_clause,
        format('FROM %s', table_oid),
        get_where_clause(
            default_predicates(query, epoch),
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


CREATE OR REPLACE FUNCTION ioql_query_local_partition_agg_limit_rows_by_only_time_sql(
    query                  ioql_query,
    epoch                  partition_epoch,
    pr                     partition_replica,
    additional_constraints TEXT DEFAULT NULL)
    RETURNS TEXT AS
$BODY$
DECLARE
    crn_row         chunk_replica_node;
    code            TEXT = '';
    index           INT = 0;
    previous_tables TEXT = NULL;
BEGIN
    FOR crn_row IN SELECT *
                   FROM get_local_chunk_replica_node_for_pr_time_desc(pr) LOOP
        IF index = 0 THEN
            code := code || format($$  WITH results_%s AS (%s) $$,
                                   index,
                                   base_query_agg_partial(query,
                                                          format('%I.%I', crn_row.schema_name,
                                                                 crn_row.table_name) :: REGCLASS,
                                                          epoch,
                                                          additional_constraints,
                                                          (query.limit_rows + 1) :: TEXT));
            previous_tables := 'SELECT * FROM results_0';
        ELSE
            --the stopping criteria is if I already have limit_rows+1 in distinct rows
            --the +1 guarantees that the limit_rows'th group was closed
            --otherwise have to try to get limit_rows rows again
            --NOTE: Limiting by query.limit_rows - count(distinct) is not right
            --      this is because my new rows may overlap with previous rows.
            code := code || format($$  , results_%s AS (%s) $$, index,
                                   base_query_agg_partial(query,
                                                          format('%I.%I', crn_row.schema_name,
                                                                 crn_row.table_name) :: REGCLASS,
                                                          epoch,
                                                          additional_constraints,
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

CREATE OR REPLACE FUNCTION ioql_query_local_partition_agg_limit_rows_by_time_and_group_sql(
    query                  ioql_query,
    epoch                  partition_epoch,
    pr                     partition_replica,
    additional_constraints TEXT DEFAULT NULL)
    RETURNS TEXT AS
$BODY$
DECLARE
    crn_row         RECORD;
    code            TEXT = '';
    index           INT = 0;
    previous_tables TEXT = '';
    prev_start_time BIGINT;
BEGIN

    --todo this join stupid a redo of  get_local_chunk_replica_node_for_pr_time_desc
    FOR crn_row IN SELECT
                       c.start_time,
                       crn.*
                   FROM get_local_chunk_replica_node_for_pr_time_desc(pr) crn
                   INNER JOIN chunk c ON (c.id = crn.chunk_id)
    LOOP
        IF index = 0 THEN
            --always scan first table
            code := code || format($$  WITH results_%s AS (%s) $$,
                                   index,
                                   base_query_agg_partial(query,
                                                          format('%I.%I', crn_row.schema_name, crn_row.table_name),
                                                          epoch,
                                                          additional_constraints,
                                                          (query.limit_rows) :: TEXT
                                   ));
            previous_tables := 'SELECT * FROM results_0';
        ELSE
            --continue going down unless the stopping criteria is met
            --stopping criteria is:
            -- 1) we already have limit_rows distinct group_field, time combinations
            -- 2) each of those top combinations belongs to a closed group.
            --    a closed group is one where the group starts after the start_time of the last table processed.
            code := code || format($$  , results_%s AS (%s) $$, index,
                                   base_query_agg_partial(query,
                                                          format('%I.%I', crn_row.schema_name, crn_row.table_name),
                                                          epoch,
                                                          additional_constraints,
                                                          format(
                                                              '(SELECT CASE WHEN COUNT(*) = %1$L AND min(group_time) > %4$L THEN 0 ELSE %1$L END '
                                                              ||
                                                              'FROM (SELECT DISTINCT %3$I, group_time FROM (%2$s) as y) as x)',
                                                              query.limit_rows,
                                                              previous_tables,
                                                              (query.aggregate).group_field,
                                                              prev_start_time
                                                          )
                                   ));
            previous_tables := previous_tables || format(' UNION ALL SELECT * FROM results_%s', index);
        END IF;
        prev_start_time := crn_row.start_time;
        index := index + 1;
    END LOOP;
    code := code || previous_tables;
    RETURN code;
END
$BODY$ LANGUAGE plpgsql STABLE;


CREATE OR REPLACE FUNCTION ioql_query_local_partition_agg_no_limit_rows_sql(
    query                  ioql_query,
    epoch                  partition_epoch,
    pr                     partition_replica,
    additional_constraints TEXT DEFAULT NULL)
    RETURNS TEXT LANGUAGE SQL AS
$BODY$
SELECT string_agg('(' || base_query_agg_partial(query,
                                                format('%I.%I', crn.schema_name, crn.table_name) :: REGCLASS,
                                                epoch,
                                                additional_constraints,
                                                NULL) || ')', ' UNION ALL ')
FROM get_local_chunk_replica_node_for_pr_time_desc(pr) crn
$BODY$;


CREATE OR REPLACE FUNCTION ioql_query_local_partition_agg_sql(
    query                  ioql_query,
    epoch                  partition_epoch,
    pr                     partition_replica,
    additional_constraints TEXT DEFAULT NULL)
    RETURNS TEXT LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF query.limit_rows IS NULL THEN
        RETURN ioql_query_local_partition_agg_no_limit_rows_sql(query, epoch, pr, additional_constraints);
    ELSE
        IF (query.aggregate).group_field IS NULL THEN
            RETURN ioql_query_local_partition_agg_limit_rows_by_only_time_sql(query, epoch, pr, additional_constraints);
        ELSE
            RETURN ioql_query_local_partition_agg_limit_rows_by_time_and_group_sql(query, epoch, pr,
                                                                                   additional_constraints);
        END IF;
    END IF;
END
$BODY$;


CREATE OR REPLACE FUNCTION get_max_time_on_partition(time_col_name NAME, part_replica partition_replica, additional_constraints TEXT)
    RETURNS BIGINT AS
$BODY$
DECLARE
    time    BIGINT;
    crn_row chunk_replica_node;
BEGIN
    time := NULL;

    FOR crn_row IN
    SELECT *
    FROM get_local_chunk_replica_node_for_pr_time_desc(part_replica) LOOP
        EXECUTE format(
            $$
                SELECT %1$I
                FROM %2$I.%3$I
                %4$s
                ORDER BY %1$I DESC NULLS LAST
                LIMIT 1
            $$, time_col_name, crn_row.schema_name, crn_row.table_name, get_where_clause(additional_constraints))
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
    epoch                  partition_epoch,
    replica_id             SMALLINT,
    additional_constraints TEXT DEFAULT NULL
)
    RETURNS TEXT AS
$BODY$
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
        FROM get_partition_replicas(epoch, replica_id) pr,
            LATERAL ioql_query_local_partition_agg_sql(query, epoch, pr, additional_constraints) AS code
        WHERE code IS NOT NULL AND code <> ''
    ) AS f;
$BODY$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION get_time_periods_limit(epoch                  partition_epoch,
                                                  replica_id             SMALLINT,
                                                  additional_constraints TEXT,
                                                  period_length          BIGINT,
                                                  num_periods            INT)
    RETURNS time_range LANGUAGE SQL STABLE AS
$BODY$
-- start and end inclusive
SELECT (get_time_periods_limit_for_max(max_time.max_time, period_length, num_periods)).*
FROM
    (
        SELECT max(get_max_time_on_partition(get_time_field(epoch.hypertable_name),pr, additional_constraints)) AS max_time
        FROM get_partition_replicas(epoch, replica_id) pr
    ) AS max_time
$BODY$;

CREATE OR REPLACE FUNCTION ioql_query_local_node_agg_grouped_sql(query                  ioql_query,
                                                                 epoch                  partition_epoch,
                                                                 replica_id             SMALLINT,
                                                                 additional_constraints TEXT DEFAULT NULL)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    trange        time_range;
    ungrouped_sql TEXT;
    grouped_sql   TEXT;
BEGIN
    IF query.limit_time_periods IS NOT NULL THEN
        trange := get_time_periods_limit(epoch,
                                         replica_id, 
                                         combine_predicates(default_predicates(query, epoch), additional_constraints),
                                         (query.aggregate).group_time,
                                         query.limit_time_periods);
        additional_constraints := combine_predicates(
            format('%1$I >= %2$L AND %1$I <=%3$L', get_time_field(query.namespace_name), trange.start_time, trange.end_time),
            additional_constraints);
    END IF;

    ungrouped_sql := ioql_query_local_node_agg_ungrouped_sql(query, epoch, replica_id, additional_constraints);

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

    RETURN grouped_sql;
END
$BODY$;



