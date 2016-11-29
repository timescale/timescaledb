CREATE OR REPLACE FUNCTION ioql_query_agg_without_limit_sql(query ioql_query, epoch partition_epoch)
    RETURNS TEXT LANGUAGE SQL STABLE AS $BODY$
--function aggregates partials across nodes.
SELECT format(
    $$
          SELECT
            %1$s
          FROM  ioql_exec_query_nodes(
                  %5$L, %6$L, %2$L
                ) as data(%2$s)
          %3$s
      $$,
    get_finalize_aggregate_sql(query.select_items, query.aggregate),
    get_partial_aggregate_column_def(query),
    get_groupby_clause(query.aggregate),
    query.namespace_name,
    query,
    epoch
)
$BODY$;

CREATE OR REPLACE FUNCTION ioql_query_agg_sql(query ioql_query, epoch partition_epoch)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
BEGIN
    IF query.limit_time_periods IS NOT NULL THEN
        RETURN format(
            $$
                with without_limit AS (
                  %1$s
                )
                SELECT without_limit.*
                FROM  without_limit,
                      get_time_periods_limit_for_max((SELECT max(time) from without_limit), %4$L, %5$L) limits
                WHERE time >= limits.start_time AND time <= limits.end_time
                %2$s
                %3$s
            $$,
            ioql_query_agg_without_limit_sql(query, epoch),
            get_orderby_clause_agg(query, 'time'),
            get_limit_clause(query.limit_rows),
            (query.aggregate).group_time,
            query.limit_time_periods
        );
    ELSE
        RETURN format(
            $$
                SELECT *
                FROM (%s) without_limit
                %s
                %s
            $$,
            ioql_query_agg_without_limit_sql(query, epoch),
            get_orderby_clause_agg(query, 'time'),
            get_limit_clause(query.limit_rows)
        );
    END IF;
END;
$BODY$
SET constraint_exclusion = ON;



