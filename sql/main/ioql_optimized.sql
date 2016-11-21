CREATE OR REPLACE FUNCTION ioql_exec_query_record_sql(query ioql_query)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    sql_code TEXT;
    epoch    partition_epoch;
BEGIN
    --TODO : broken; assumes one partition_epoch. Needs to be a loop.
    SELECT *
    INTO epoch
    FROM partition_epoch pe
    WHERE pe.hypertable_name = query.namespace_name;

    IF epoch IS NULL THEN
        RETURN format($$ SELECT * FROM no_cluster_table(%L) $$, _query);
    END IF;

    IF NOT query.aggregate IS NULL THEN
        sql_code := ioql_query_agg_sql(query, epoch);
        RAISE NOTICE E'Cross-node SQL:\n%\n', sql_code;
        RETURN sql_code;
    ELSE
        sql_code := ioql_query_nonagg_sql(query, epoch);
        RAISE NOTICE E'Cross-node SQL:\n%\n', sql_code;
        RETURN sql_code;
    END IF;
END
$BODY$;

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



