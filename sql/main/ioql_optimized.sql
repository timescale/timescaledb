CREATE OR REPLACE FUNCTION ioql_exec_query_record_sql(_query ioql_query)
    RETURNS TEXT AS $BODY$
BEGIN
    IF get_cluster_table(_query.namespace_name) IS NULL THEN
        RETURN format($$ SELECT * FROM no_cluster_table(%L) $$, _query);
    END IF;

    IF _query.aggregate IS NULL THEN
        RETURN format(
            $$
                SELECT *
                FROM ioql_exec_query_nonagg(%L) as ans(%s)
                ORDER BY time DESC NULLS LAST
            $$, _query, get_result_column_def_list_nonagg(_query));
    ELSE
        RETURN format(
            $$
                SELECT *
                FROM ioql_exec_query_agg(%L) as ans(%s)
                ORDER BY time DESC NULLS LAST
            $$,
            _query,
            get_result_column_def_list_agg(_query));
    END IF;
END
$BODY$
LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION ioql_exec_query_record_cursor(query ioql_query, curs REFCURSOR)
    RETURNS REFCURSOR AS $BODY$
BEGIN
    OPEN curs FOR EXECUTE ioql_exec_query_record_sql(query);
    RETURN curs;
END
$BODY$
LANGUAGE plpgsql STABLE;


CREATE OR REPLACE FUNCTION ioql_exec_query(query ioql_query)
    RETURNS TABLE(json TEXT) AS $BODY$
BEGIN
    --  IF to_regclass(get_cluster_name(get_namespace(query))::cstring) IS NULL THEN
    --    RETURN QUERY SELECT * FROM no_cluster_table(query);
    --    RETURN;
    --  END IF;
    RETURN QUERY EXECUTE format(
        $$
    SELECT row_to_json(ans)::text
    FROM (%s) as ans
    $$, ioql_exec_query_record_sql(query));
END
$BODY$
LANGUAGE plpgsql STABLE;



