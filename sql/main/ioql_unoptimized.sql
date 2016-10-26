DROP TYPE IF EXISTS time_range CASCADE;
CREATE TYPE time_range AS (start_time BIGINT, end_time BIGINT);

CREATE OR REPLACE FUNCTION get_cluster_name(namespace_type)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT format($$cluster."%s_%s_%s"$$, $1.project_id, $1.name, $1.replica_no)
$BODY$;

CREATE OR REPLACE FUNCTION get_cluster_table(namespace_type)
    RETURNS REGCLASS LANGUAGE SQL STABLE AS
$BODY$
SELECT get_cluster_name($1) :: REGCLASS
$BODY$;

CREATE OR REPLACE FUNCTION get_distinct_table_local(ns namespace_type)
    RETURNS REGCLASS LANGUAGE SQL STABLE AS
$BODY$
SELECT get_distinct_table_oid(ns.name)
$BODY$;

CREATE OR REPLACE FUNCTION get_distinct_table_cluster(ns namespace_type)
    RETURNS REGCLASS LANGUAGE SQL STABLE AS
$BODY$
SELECT ('cluster.' || table_name :: TEXT) :: REGCLASS
FROM get_distinct_table(ns.project_id, ns.name, ns.replica_no)
$BODY$;


CREATE OR REPLACE FUNCTION get_distinct_values_local(ns namespace_type, field TEXT)
    RETURNS SETOF TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    table_name REGCLASS;
BEGIN
    SELECT dis.table_name
    INTO table_name
    FROM distinct_tables dis
    WHERE project_id = ns.project_id AND namespace = ns.name AND replica_no = ns.replica_no;
    RETURN QUERY EXECUTE format(
        $$
            SELECT DISTINCT value
            FROM %s
            WHERE field = %L
        $$,
        table_name, field);
END
$BODY$;

CREATE OR REPLACE FUNCTION get_distinct_values_cluster(ns namespace_type, field TEXT)
    RETURNS SETOF TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
BEGIN
    RETURN QUERY EXECUTE format(
        $$
            SELECT DISTINCT value
            FROM %s
            WHERE field = %L
        $$,
        get_distinct_table_cluster(ns), field);
END
$BODY$;

CREATE OR REPLACE FUNCTION get_field_is_distinct(ns namespace_type, field_name TEXT)
    RETURNS BOOLEAN LANGUAGE PLPGSQL STABLE STRICT AS
$BODY$
DECLARE
    field_is_distinct BOOLEAN;
BEGIN
    SELECT DISTINCT is_distinct
    INTO STRICT field_is_distinct
    FROM cluster.project_field pf
    WHERE
        pf.project_id = ns.project_id AND
        pf.namespace = ns.name AND
        pf.field = field_name;
    RETURN field_is_distinct;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'Field is_distinct not found. Field name: % namespace: %', field_name, ns;
END
$BODY$;

CREATE OR REPLACE FUNCTION get_field_type(ns namespace_type, field_name TEXT)
    RETURNS REGTYPE LANGUAGE PLPGSQL STABLE STRICT AS
$BODY$
DECLARE
    field_type REGTYPE;
BEGIN
    SELECT DISTINCT value_type
    INTO STRICT field_type
    FROM cluster.project_field pf
    WHERE
        pf.project_id = ns.project_id AND
        pf.namespace = ns.name AND
        pf.field = field_name;
    RETURN field_type;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'Field type not found. Field name: % namespace: %', field_name, ns;
END
$BODY$;

CREATE OR REPLACE FUNCTION get_field_value_specifier(field_name TEXT)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS
$BODY$
SELECT format('%I', field_name);
$BODY$;

CREATE OR REPLACE FUNCTION get_field_value_specifier(ns namespace_type, field_name TEXT)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS
$BODY$
SELECT get_field_value_specifier(field_name)
$BODY$;

CREATE OR REPLACE FUNCTION is_partition_key(ns namespace_type, field_name TEXT)
    RETURNS BOOLEAN LANGUAGE PLPGSQL STABLE STRICT AS
$BODY$
DECLARE
    ret BOOLEAN;
BEGIN
    SELECT DISTINCT is_partition_key
    INTO STRICT ret
    FROM cluster.project_field pf
    WHERE
        pf.project_id = ns.project_id AND
        pf.namespace = ns.name AND
        pf.field = field_name;
    RETURN ret;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RAISE EXCEPTION 'Field is_partition_key not found. Field name: % namespace: %', field_name, ns;
END
$BODY$;

CREATE OR REPLACE FUNCTION get_field_name_clause(select_field TEXT)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT CASE WHEN select_field IS NOT NULL AND select_field <> '*' THEN select_field END;
$BODY$;


CREATE OR REPLACE FUNCTION get_group_field_clause(ns namespace_type, group_field TEXT)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT CASE WHEN group_field IS NOT NULL THEN get_field_value_specifier(ns, group_field)
       ELSE 'NULL' END;
$BODY$;

CREATE OR REPLACE FUNCTION get_value_clause(ns namespace_type, select_field TEXT, op TEXT)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT CASE WHEN op IS NULL THEN
    CASE WHEN select_field = '*' THEN 'value'
    ELSE format('jsonb_build_object(%L, %s)', select_field, get_field_value_specifier(ns, select_field))
    END
       ELSE
           format('%s(%s)', op, get_field_value_specifier(ns, select_field))
       END;
$BODY$;


CREATE OR REPLACE FUNCTION get_value_type(ns namespace_type, select_field TEXT, op TEXT)
    RETURNS REGTYPE LANGUAGE SQL STABLE AS
$BODY$
SELECT CASE WHEN op IS NULL THEN
    CASE WHEN select_field = '*' THEN 'jsonb' :: REGTYPE
    ELSE get_field_type(ns, select_field)
    END
       ELSE
           'double precision' :: REGTYPE
       END;
$BODY$;

CREATE OR REPLACE FUNCTION get_value_type_clause(ns namespace_type, select_field TEXT, op TEXT)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT format('%L::regtype', get_value_type(ns, select_field, op) :: TEXT);
$BODY$;


CREATE OR REPLACE FUNCTION get_time_clause(group_time BIGINT)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT CASE WHEN group_time IS NOT NULL THEN format('(time - (time %% %L::bigint))::bigint', group_time)
       ELSE 'time' END;
$BODY$;

CREATE OR REPLACE FUNCTION get_group_field_clause(ns namespace_type, op TEXT, group_field TEXT)
    RETURNS TEXT LANGUAGE SQL STABLE AS $BODY$
SELECT CASE WHEN op IS NOT NULL THEN get_field_value_specifier(ns, group_field)
       ELSE 'NULL' END;
$BODY$;

CREATE OR REPLACE FUNCTION get_full_select_clause(ns namespace_type, query ioql_query)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT format('SELECT time, %s', get_field_list(query));
$BODY$;

CREATE OR REPLACE FUNCTION get_time_predicate(cond time_condition_type)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT string_agg(clauses.val, ' AND ')
FROM (VALUES ('time>=' || cond.from_time), ('time<' || cond.to_time)) AS clauses(val);
$BODY$;

CREATE OR REPLACE FUNCTION get_select_field_required_field(select_field select_item [])
    RETURNS SETOF TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT CASE WHEN select_field <> NULL THEN
    (SELECT field
     FROM unnest(select_field))
       END;
$BODY$;

CREATE OR REPLACE FUNCTION get_field_condition_required_fields(cond field_condition_type)
    RETURNS SETOF TEXT LANGUAGE SQL IMMUTABLE STRICT AS $BODY$
SELECT p.field
FROM unnest(cond.predicates) AS p
WHERE cond.conjunctive = 'AND'
$BODY$;

CREATE OR REPLACE FUNCTION get_required_fields(query ioql_query)
    RETURNS SETOF TEXT LANGUAGE SQL IMMUTABLE STRICT AS
$BODY$
(
    SELECT *
    FROM get_field_condition_required_fields(query.field_condition)
)
UNION
(
    SELECT *
    FROM get_select_field_required_field(query.select_field) f
    WHERE f IS NOT NULL
)
$BODY$;

CREATE OR REPLACE FUNCTION get_required_fields_predicate(data_table TEXT, query ioql_query)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS
$BODY$
SELECT format(
    'NOT EXISTS ((SELECT * FROM get_required_fields(%2$L)) EXCEPT ((SELECT field_name FROM data_fields WHERE table_name = %1$L::regclass)))',
    data_table, query);
$BODY$;

CREATE OR REPLACE FUNCTION get_select_field_predicate(items select_item [])
    RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS
$BODY$
SELECT string_agg(format('%I IS NOT NULL', field), ' AND ')
FROM unnest(items);
$BODY$;

CREATE OR REPLACE FUNCTION get_partitioning_predicate(ns                       namespace_type,
                                                      partitioning_field_value TEXT,
                                                      total_partitions         INT)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS
$BODY$
SELECT format('get_partition_for_key(%I, %L) = %L', get_partitioning_field(ns.project_id :: INT, ns.name),
              total_partitions, get_partition_for_key(partitioning_field_value, total_partitions))
$BODY$;

CREATE OR REPLACE FUNCTION get_partitioning_field_value(ns namespace_type, cond field_condition_type)
    RETURNS TEXT LANGUAGE SQL STABLE STRICT AS
$BODY$
SELECT p.constant
FROM unnest(cond.predicates) AS p
WHERE is_partition_key(ns, p.field) AND cond.conjunctive = 'AND' AND p.op = '='
$BODY$;

CREATE OR REPLACE FUNCTION get_one_field_predicate_clause(ns namespace_type, field_pred field_predicate)
    RETURNS TEXT LANGUAGE SQL STABLE STRICT AS
$BODY$
SELECT format('(%1$s%2$s%3$L AND %1$s IS NOT NULL)', get_field_value_specifier(ns, field_pred.field), field_pred.op,
              field_pred.constant)
$BODY$;

CREATE OR REPLACE FUNCTION get_field_predicate_clause(ns namespace_type, cond field_condition_type)
    RETURNS TEXT LANGUAGE SQL STABLE STRICT AS
$BODY$
SELECT '(' || string_agg(field_info.where_pred, ' ' || cond.conjunctive || ' ') || ')'
FROM unnest(cond.predicates) AS p,
LATERAL (
SELECT *
FROM get_one_field_predicate_clause(ns, p)
) AS field_info(where_pred);
$BODY$;

CREATE OR REPLACE FUNCTION combine_predicates(VARIADIC clauses TEXT [])
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT string_agg(clause.val, ' AND ')
FROM unnest(clauses) AS clause(val);
$BODY$;

CREATE OR REPLACE FUNCTION get_where_clause(VARIADIC clauses TEXT [])
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT coalesce('WHERE ' || combine_predicates(VARIADIC clauses), '');
$BODY$;

CREATE OR REPLACE FUNCTION get_json_aggregate_field_key(agg aggregate_type, select_i select_item)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT format('%s(%s)', lower(select_i.func :: TEXT), select_i.field);
$BODY$;

CREATE OR REPLACE FUNCTION get_groupby_clause(agg aggregate_type)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS
$BODY$
SELECT CASE
       WHEN agg IS NULL THEN
           NULL
       WHEN agg.group_field IS NOT NULL THEN
           format('GROUP BY %s, group_time', agg.group_field)
       ELSE
           'GROUP BY group_time'
       END;
$BODY$;
--
CREATE OR REPLACE FUNCTION get_groupby_clause(ns namespace_type, agg aggregate_type)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT get_groupby_clause(agg);
$BODY$;

CREATE OR REPLACE FUNCTION get_limit_clause(limit_count INT)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT 'LIMIT ' || limit_count
$BODY$;


CREATE OR REPLACE FUNCTION get_agg_orderby_clause(query ioql_query, time_col TEXT = 'group_time')
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT CASE
       WHEN query.limit_rows IS NULL THEN
           NULL --no need to order if not gonna limit
       WHEN (query.aggregate).group_field IS NOT NULL THEN
           format('ORDER BY %s DESC NULLS LAST, %s', time_col,
                  (query.aggregate).group_field) --group field needs to be included so that 5 aggregates across partitions overlap
       ELSE
           format('ORDER BY %s DESC NULLS LAST', time_col)
       END;
$BODY$;


CREATE OR REPLACE FUNCTION get_orderby_clause(query ioql_query)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT CASE
       WHEN query.limit_by_field IS NOT NULL THEN
           'ORDER BY time DESC NULLS LAST, ' || (query.limit_by_field).field
       ELSE
           'ORDER BY time DESC NULLS LAST'
       END;
$BODY$;


CREATE OR REPLACE FUNCTION default_predicates(ns namespace_type, query ioql_query, total_partitions INT = NULL)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT combine_predicates(
    get_time_predicate(query.time_condition),
    get_field_predicate_clause(ns, query.field_condition),
    get_select_field_predicate(query.select_field),
    get_partitioning_predicate(ns, get_partitioning_field_value(ns, query.field_condition), total_partitions)
);
$BODY$;


CREATE OR REPLACE FUNCTION base_query_raw(select_clause   TEXT, from_clause TEXT, where_clause TEXT,
                                          group_by_clause TEXT, order_by_clause TEXT, limit_clause TEXT)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT format(
    $$
        %s
        %s
        %s
        %s
        %s
        %s
    $$,
    select_clause,
    from_clause,
    where_clause,
    group_by_clause,
    order_by_clause,
    limit_clause
);
$BODY$;

CREATE OR REPLACE FUNCTION base_query_builder(query                  ioql_query, data_table REGCLASS,
                                              additional_constraints TEXT, select_clause TEXT, limit_clause TEXT,
                                              total_partitions       INT = NULL)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    ns          namespace_type;
    from_clause TEXT;
BEGIN
    ns := ROW (query.project_id, query.namespace_name, 1);
    from_clause := format('FROM %s', data_table);

    RETURN base_query_raw(
        select_clause,
        from_clause,
        get_where_clause(
            default_predicates(ns, query, total_partitions),
            additional_constraints
        --get_required_fields_predicate(data_table, query) --this may be run on parent tables
        ),
        get_groupby_clause(ns, query.aggregate),
        get_orderby_clause(query),
        limit_clause);
END
$BODY$;

CREATE OR REPLACE FUNCTION base_query(query ioql_query, data_table REGCLASS, additional_constraints TEXT DEFAULT NULL)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    ns            namespace_type;
    select_clause TEXT;
BEGIN
    ns := ROW (query.project_id, query.namespace_name, 1);
    RETURN base_query_builder(query, data_table, additional_constraints, get_full_select_clause(ns, query),
                              get_limit_clause(query.limit_rows));
END
$BODY$;

CREATE OR REPLACE FUNCTION limit_by_every_query_nonagg(query ioql_query, data_table REGCLASS)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    ns        namespace_type;
    query_sql TEXT;
BEGIN
    ns := ROW (query.project_id, query.namespace_name, 1);
    query_sql = format(
        $$
            SELECT namespace, field_name, group_field, ret_time, ret_value, value_type
            FROM
            (
                SELECT ROW_NUMBER() OVER (PARTITION BY group_field ORDER BY ret_time DESC NULLS LAST, ret_value) as rank, *
                FROM
                (
                    %1$s
                ) as res
            ) as o
            WHERE rank <= %2$L and get_field_is_distinct(%4$L, %5$L)
            ORDER BY ret_time DESC NULLS LAST, group_field
            %3$s
        $$,
        base_query_builder(
            query,
            data_table,
            NULL,
            format(
                $$
                    SELECT %L::text as namespace,
                    %L::text as field_name,
                    %s::text as group_field,
                    %s::bigint as ret_time,
                    %s::text as ret_value,
                    %s::regtype value_type
                $$,
                query.namespace_name,
                get_field_name_clause(query.select_field),
                get_field_value_specifier(ns, (query.limit_by_field).field),
                get_time_clause((query.aggregate).group_time),
                get_value_clause(ns, query.select_field, (query.aggregate).func),
                get_value_type_clause(ns, query.select_field, (query.aggregate).func)
            ),
            NULL
        ),
        (query.limit_by_field).count,
        get_limit_clause(query.limit_rows),
        ns,
        (query.limit_by_field).field
    );

    RETURN query_sql;
END
$BODY$;

CREATE OR REPLACE FUNCTION get_time_periods_limit_for_max(max_time BIGINT, period_length BIGINT, num_periods INT)
    RETURNS time_range LANGUAGE SQL STABLE AS
$BODY$
--todo unit test;
-- start and end inclusive
SELECT ROW (
       (max_time - (max_time % period_length)) - (period_length :: BIGINT * (num_periods - 1) :: BIGINT),
       max_time
) :: time_range;
$BODY$;

CREATE OR REPLACE FUNCTION group_field_agg(query                  ioql_query,
                                           data_table             REGCLASS,
                                           additional_constraints TEXT DEFAULT NULL)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    ns        namespace_type;
    query_sql TEXT;
BEGIN
    ns := ROW (query.project_id, query.namespace_name, 1);
    query_sql = format(
        $$
            SELECT %6$L::text, %7$L::text, res.group_field as group_field, res.ret_time, res.ret_value, res.value_type
            FROM
            (
                %3$s
            ) AS res
            %5$s
        $$,
        data_table,
        (query.aggregate).group_field,
        base_query(query, data_table, additional_constraints),
        ns,
        get_limit_clause(query.limit_rows),
        query.namespace_name,
        get_field_name_clause(query.select_field)
    );
    RETURN query_sql;
END
$BODY$;


CREATE OR REPLACE FUNCTION agg_query_number_time_periods_agg(query ioql_query, data_table REGCLASS)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    ns        namespace_type;
    query_sql TEXT;
BEGIN
    ns := ROW (query.project_id, query.namespace_name, 1);
    query_sql = format(
        $$
            SELECT res.*
            FROM
            (
                SELECT periods.*
                FROM %3$s as dt,
                get_time_periods_limit_for_max(dt.time, %1$L, %2$L) as periods
                %4$s
                ORDER BY time DESC LIMIT 1
            ) as time_range,
            LATERAL (%5$s) res
        $$,
        (query.aggregate).group_time,
        query.limit_time_periods,
        data_table,
        get_where_clause(default_predicates(ns, query)),
        agg_query_regular_limit(query, data_table,
                                format('time <= time_range.end_time AND time >= time_range.start_time'))
    );
    RETURN query_sql;
END
$BODY$;

CREATE OR REPLACE FUNCTION agg_query_regular_limit(query                 ioql_query, data_table REGCLASS,
                                                   additional_constraint TEXT DEFAULT NULL)
    RETURNS TEXT LANGUAGE SQL STABLE AS $BODY$
SELECT CASE
       WHEN (query.aggregate).group_field IS NOT NULL THEN group_field_agg(query, data_table, additional_constraint)
       ELSE base_query(query, data_table, additional_constraint)
       END;
$BODY$;

CREATE OR REPLACE FUNCTION agg_query(query ioql_query, data_table REGCLASS)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT CASE
       WHEN query.limit_time_periods IS NOT NULL THEN
           agg_query_number_time_periods_agg(query, data_table)
       ELSE
           agg_query_regular_limit(query, data_table)
       END;
$BODY$;

CREATE OR REPLACE FUNCTION get_json_build_object_sql(query ioql_query)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT ' jsonb_build_object( ' ||
       string_agg(
           get_json_aggregate_field_key(query.aggregate, i) || ', ' || i.field,
           ', '
       ) || ')'
FROM unnest(query.select_field) AS i
$BODY$;


CREATE OR REPLACE FUNCTION full_query(query ioql_query, data_table REGCLASS)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT CASE
       WHEN NOT (query.aggregate IS NULL) THEN
           CASE
           WHEN (query.aggregate).group_field IS NOT NULL THEN
               format(
                   $$
                        SELECT
                            namespace,
                            field_name,
                            group_field,
                            ret_time,
                            %s:text,
                            return_value_type
                        FROM (%s) as nj(namespace, field_name, group_field, ret_time, ret_value, return_value_type)
                        ORDER BY ret_time DESC NULLS LAST, group_field
                    $$,
                   (query.aggregate).group_field, get_json_build_object_sql(query), agg_query(query, data_table))
           ELSE
               format(
                   $$
                        SELECT namespace, field_name, group_field, ret_time, %s::text, return_value_type
                        FROM (%s) as  nj(namespace, field_name, group_field, ret_time, ret_value, return_value_type)
                        ORDER BY ret_time DESC NULLS LAST, group_field
                    $$,
                   get_json_build_object_sql(query), agg_query(query, data_table))
           END
       WHEN query.limit_by_field IS NOT NULL THEN
           limit_by_every_query_nonagg(query, data_table)
       ELSE
           base_query(query, data_table)
       END;
$BODY$;


CREATE OR REPLACE FUNCTION no_cluster_table(query ioql_query)
    RETURNS TABLE(json TEXT) LANGUAGE PLPGSQL STABLE AS
$BODY$
BEGIN
    IF EXISTS(
        SELECT 1
        FROM system.namespaces
        WHERE name = query.namespace_name AND project_id = query.project_id
    ) THEN
        RAISE NOTICE 'empty result, cluster table not found %', query;
        RETURN;
    ELSE
        RAISE EXCEPTION 'Namespace ''%'' does not exist in project ''%'' ', query.namespace_name, query.project_id;
    END IF;
END
$BODY$;


CREATE OR REPLACE FUNCTION ioql_exec_query_unoptimized(query ioql_query)
    RETURNS TABLE(json TEXT) LANGUAGE PLPGSQL STABLE AS
$BODY$
BEGIN
    IF to_regclass(get_cluster_name(get_namespace(query)) :: cstring) IS NULL THEN
        RETURN QUERY SELECT *
                     FROM no_cluster_table(query);
        RETURN;
    END IF;
    RAISE NOTICE 'Cross-node query %', full_query(query, get_cluster_table(get_namespace(query)));
    RETURN QUERY EXECUTE full_query(query, get_cluster_table(get_namespace(query)));
END
$BODY$;




