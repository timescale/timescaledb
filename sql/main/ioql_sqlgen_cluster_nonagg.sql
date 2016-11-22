CREATE OR REPLACE FUNCTION ioql_query_nonagg_without_limit_sql(query ioql_query, epoch partition_epoch)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE AS $BODY$
--function aggregates partials across nodes.
BEGIN
    --todo: the order by and limit can be removed?
    RETURN format(
        $$
            SELECT *
            FROM  ioql_exec_query_nodes(
                %2$L, %5$L, %1$L
              ) as res(%1$s)
            ORDER BY time DESC NULLS LAST
            %4$s
        $$,
        get_result_column_def_list_nonagg(query),
        query,
        query.namespace_name,
        get_limit_clause(query.limit_rows),
        epoch
    );
END
$BODY$;

CREATE OR REPLACE FUNCTION ioql_query_nonagg_sql(query ioql_query, epoch partition_epoch)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE AS $BODY$
DECLARE
BEGIN
    IF query.limit_by_field IS NOT NULL THEN
        RETURN format(
            $$
                SELECT %4$s
                FROM (
                    SELECT
                        ROW_NUMBER() OVER (PARTITION BY %2$s ORDER BY time DESC NULLS LAST) AS rank,
                         *
                    FROM  (
                      %5$s
                    ) as ieq
                ) as ranked
                WHERE rank <= %7$L OR time IS NULL
                ORDER BY time DESC NULLS LAST, %2$s
                %6$s
            $$,
            get_result_column_def_list_nonagg(query),
            (query.limit_by_field).field,
            query,
            get_result_column_list_nonagg(query),
            ioql_query_nonagg_without_limit_sql(query, epoch),
            get_limit_clause(query.limit_rows),
            (query.limit_by_field).count
        );
    ELSE
        RETURN format(
            $$
                SELECT *
                FROM  (
                  %2$s
                ) as ieq(%1$s)
                ORDER BY time DESC NULLS LAST
                %3$s
            $$,
            get_result_column_list_nonagg(query),
            ioql_query_nonagg_without_limit_sql(query, epoch),
            get_limit_clause(query.limit_rows)
        );
    END IF;
END;
$BODY$;
