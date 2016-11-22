CREATE OR REPLACE FUNCTION get_time_clause(group_time BIGINT)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT CASE
       WHEN group_time IS NOT NULL THEN
           format('(time - (time %% %L::bigint))::bigint', group_time)
       ELSE
           'time'
       END;
$BODY$;

CREATE OR REPLACE FUNCTION get_full_select_clause_nonagg(query ioql_query)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT format('SELECT %s', get_result_column_list_nonagg(query));
$BODY$;

CREATE OR REPLACE FUNCTION get_from_clause(crn chunk_replica_node)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT format('FROM %I.%I', crn.schema_name, crn.table_name);
$BODY$;

/* predicates */
CREATE OR REPLACE FUNCTION combine_predicates(VARIADIC clauses TEXT [])
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT string_agg(format('( %s )', clause.val), ' AND ')
FROM unnest(clauses) AS clause(val)
WHERE val IS NOT NULL;
$BODY$;

CREATE OR REPLACE FUNCTION get_time_predicate(cond time_condition_type)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT string_agg(clauses.val, ' AND ')
FROM (VALUES ('time>=' || cond.from_time), ('time<' || cond.to_time)) AS clauses(val);
$BODY$;

--TODO: Review this
CREATE OR REPLACE FUNCTION get_select_field_predicate(items select_item [])
    RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS
$BODY$
SELECT string_agg(format('%I IS NOT NULL', field), ' AND ')
FROM unnest(items);
$BODY$;


CREATE OR REPLACE FUNCTION get_one_field_predicate_clause(field_pred field_predicate)
    RETURNS TEXT LANGUAGE SQL STABLE STRICT AS
$BODY$
SELECT format('(%1$I%2$s%3$L AND %1$I IS NOT NULL)', field_pred.field, field_pred.op, field_pred.constant)
$BODY$;

CREATE OR REPLACE FUNCTION get_field_predicate_clause(cond field_condition_type)
    RETURNS TEXT LANGUAGE SQL STABLE STRICT AS
$BODY$
SELECT '( ' || string_agg(get_one_field_predicate_clause(p), format(' %s ', cond.conjunctive)) || ' )'
FROM unnest(cond.predicates) AS p
$BODY$;

CREATE OR REPLACE FUNCTION get_constrained_partitioning_field_value(partitioning_field_name NAME,
                                                                    cond                    field_condition_type)
    RETURNS TEXT LANGUAGE SQL STABLE STRICT AS
$BODY$
SELECT p.constant
FROM unnest(cond.predicates) AS p
WHERE p.field = partitioning_field_name AND cond.conjunctive = 'AND' AND p.op = '='
$BODY$;

CREATE OR REPLACE FUNCTION get_partitioning_predicate(
    query ioql_query,
    epoch partition_epoch
)
    RETURNS TEXT LANGUAGE PLPGSQL IMMUTABLE STRICT AS
$BODY$
DECLARE
    keyspace_value SMALLINT;
    field_value    TEXT;
BEGIN
    field_value := get_constrained_partitioning_field_value(epoch.partitioning_field, query.field_condition);

    EXECUTE format($$ SELECT %s(%L, %L) $$, epoch.partitioning_func, field_value, epoch.partitioning_mod)
    INTO keyspace_value;

    IF field_value IS NOT NULL THEN
        RETURN format('%s(%I, %L) = %L',
                      epoch.partitioning_func,
                      epoch.partitioning_field,
                      epoch.partitioning_mod,
                      keyspace_value);
    END IF;
    RETURN NULL;
END
$BODY$;

CREATE OR REPLACE FUNCTION default_predicates(query ioql_query, epoch partition_epoch)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT combine_predicates(
    get_time_predicate(query.time_condition),
    get_field_predicate_clause(query.field_condition),
    get_select_field_predicate(query.select_items),
    get_partitioning_predicate(query, epoch)
);
$BODY$;

CREATE OR REPLACE FUNCTION get_where_clause(VARIADIC clauses TEXT [])
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT coalesce('WHERE ' || combine_predicates(VARIADIC clauses), '');
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


CREATE OR REPLACE FUNCTION get_orderby_clause_nonagg(query ioql_query)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT CASE
       WHEN query.limit_by_field IS NOT NULL THEN
           'ORDER BY time DESC NULLS LAST, ' || (query.limit_by_field).field
       ELSE
           'ORDER BY time DESC NULLS LAST'
       END;
$BODY$;

CREATE OR REPLACE FUNCTION get_orderby_clause_agg(query ioql_query, time_col TEXT = 'group_time')
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT CASE
       WHEN query.limit_rows IS NULL THEN
           NULL --no need to order if not gonna limit
       WHEN (query.aggregate).group_field IS NOT NULL THEN
           format('ORDER BY %s DESC NULLS LAST, %s',
                  time_col,
                  (query.aggregate).group_field) --group field needs to be included so that 5 aggregates across partitions overlap
       ELSE
           format('ORDER BY %s DESC NULLS LAST', time_col)
       END;
$BODY$;

CREATE OR REPLACE FUNCTION get_limit_clause(limit_count INT)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT 'LIMIT ' || limit_count
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
