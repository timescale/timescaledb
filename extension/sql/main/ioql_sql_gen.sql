CREATE OR REPLACE FUNCTION get_time_clause(time_col_name NAME, time_col_type regtype, group_time BIGINT)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT CASE
       WHEN group_time IS NULL THEN
           format('%I', time_col_name)
       WHEN time_col_type IN ('BIGINT', 'INTEGER', 'SMALLINT') THEN
           format('(%1$I - (%1$I %% %2$L::%3$s))::%3$s', time_col_name, group_time, time_col_type)
       WHEN time_col_type IN ('TIMESTAMPTZ'::REGTYPE) THEN
           format('to_timestamp( (
                  (EXTRACT(EPOCH from %1$I)*1e6)::bigint -
                  ( (EXTRACT(EPOCH from %1$I)*1e6)::bigint %% %2$L::bigint )
              )::double precision / 1e6
            )', time_col_name, group_time) --group time is given in us
       WHEN time_col_type IN ('TIMESTAMP'::REGTYPE) THEN
           --This converts the timestamp to a timestampz, then converts to epoch in UTC time, and then
           --converts back to a timestamp. All conversions done using the timezone setting.
           --A consequence is that the mod is taken with respect to UTC time.
           format('to_timestamp( (
                  (EXTRACT(EPOCH from %1$I::timestamptz)*1e6)::bigint -
                  ( (EXTRACT(EPOCH from %1$I::timestamptz)*1e6)::bigint %% %2$L::bigint )
              )::double precision / 1e6
            )::timestamp', time_col_name, group_time) --group time is given in us
       END;
$BODY$;

CREATE OR REPLACE FUNCTION get_full_select_clause_nonagg(query ioql_query)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT format('SELECT %s', get_result_column_list_nonagg(query));
$BODY$;

CREATE OR REPLACE FUNCTION get_from_clause(
    crn _iobeamdb_catalog.chunk_replica_node
)
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

CREATE OR REPLACE FUNCTION get_time_predicate(time_col_name NAME, time_col_type regtype, cond time_condition_type)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT string_agg(clauses.val, ' AND ')
FROM (
       VALUES (format('%I >= ', time_col_name) ||  NULLIF(_sysinternal.time_literal_sql(cond.from_time, time_col_type), 'NULL')),
              (format('%I < ', time_col_name)  ||  NULLIF(_sysinternal.time_literal_sql(cond.to_time, time_col_type), 'NULL'))
     ) AS clauses(val);
$BODY$;

--TODO: Review this
CREATE OR REPLACE FUNCTION get_select_column_predicate(items select_item [])
    RETURNS TEXT LANGUAGE SQL IMMUTABLE STRICT AS
$BODY$
SELECT string_agg(format('%I IS NOT NULL', column_name), ' AND ')
FROM unnest(items);
$BODY$;


CREATE OR REPLACE FUNCTION get_one_column_predicate_clause(column_pred column_predicate)
    RETURNS TEXT LANGUAGE SQL STABLE STRICT AS
$BODY$
SELECT format('(%1$I%2$s%3$L AND %1$I IS NOT NULL)', column_pred.column_name, column_pred.op, column_pred.constant)
$BODY$;

CREATE OR REPLACE FUNCTION get_column_predicate_clause(cond column_condition_type)
    RETURNS TEXT LANGUAGE SQL STABLE STRICT AS
$BODY$
SELECT '( ' || string_agg(get_one_column_predicate_clause(p), format(' %s ', cond.conjunctive)) || ' )'
FROM unnest(cond.predicates) AS p
$BODY$;

CREATE OR REPLACE FUNCTION get_constrained_partitioning_column_value(partitioning_column_name NAME,
                                                                    cond                    column_condition_type)
    RETURNS TEXT LANGUAGE SQL STABLE STRICT AS
$BODY$
SELECT p.constant
FROM unnest(cond.predicates) AS p
WHERE p.column_name = partitioning_column_name AND cond.conjunctive = 'AND' AND p.op = '='
$BODY$;

CREATE OR REPLACE FUNCTION get_partitioning_predicate(
    query ioql_query,
    epoch _iobeamdb_catalog.partition_epoch
)
    RETURNS TEXT LANGUAGE PLPGSQL IMMUTABLE STRICT AS
$BODY$
DECLARE
    keyspace_value SMALLINT;
    column_value    TEXT;
BEGIN
    column_value := get_constrained_partitioning_column_value(epoch.partitioning_column, query.column_condition);

    EXECUTE format($$ SELECT %s(%L, %L) $$, epoch.partitioning_func, column_value, epoch.partitioning_mod)
    INTO keyspace_value;

    IF column_value IS NOT NULL THEN
        RETURN format('%s(%I, %L) = %L',
                      epoch.partitioning_func,
                      epoch.partitioning_column,
                      epoch.partitioning_mod,
                      keyspace_value);
    END IF;
    RETURN NULL;
END
$BODY$;

CREATE OR REPLACE FUNCTION default_predicates(
    query ioql_query,
    epoch _iobeamdb_catalog.partition_epoch
)
    RETURNS TEXT LANGUAGE SQL STABLE AS
$BODY$
SELECT combine_predicates(
    get_time_predicate(get_time_column(query.hypertable_name), get_time_column_type(query.hypertable_name), query.time_condition),
    get_column_predicate_clause(query.column_condition),
    get_select_column_predicate(query.select_items),
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
       WHEN agg.group_column IS NOT NULL THEN
           format('GROUP BY %s, group_time', agg.group_column)
       ELSE
           'GROUP BY group_time'
       END;
$BODY$;


CREATE OR REPLACE FUNCTION get_orderby_clause_nonagg(query ioql_query)
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT CASE
       WHEN query.limit_by_column IS NOT NULL THEN
           format('ORDER BY %I DESC NULLS LAST, %s', get_time_column(query.hypertable_name), (query.limit_by_column).column_name)
	  ELSE
           format('ORDER BY %I DESC NULLS LAST', get_time_column(query.hypertable_name))
       END;
$BODY$;

CREATE OR REPLACE FUNCTION get_orderby_clause_agg(query ioql_query, time_col TEXT = 'group_time')
    RETURNS TEXT LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT CASE
       WHEN query.limit_rows IS NULL THEN
           NULL --no need to order if not gonna limit
       WHEN (query.aggregate).group_column IS NOT NULL THEN
           format('ORDER BY %s DESC NULLS LAST, %s',
                  time_col,
                  (query.aggregate).group_column) --group column needs to be included so that 5 aggregates across partitions overlap
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
