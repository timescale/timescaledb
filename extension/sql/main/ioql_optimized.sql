CREATE OR REPLACE FUNCTION ioql_exec_query_record_sql(query ioql_query)
    RETURNS TEXT LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    sql_code TEXT;
    inner_sql TEXT;
BEGIN
    --TODO : cross-epoch queries can be optimized much more than a simple limit.
    SELECT code_epoch.code
    INTO inner_sql
    FROM (
      SELECT CASE WHEN  NOT query.aggregate IS NULL THEN
                    ioql_query_agg_sql(query, pe)
                  ELSE
                     ioql_query_nonagg_sql(query, pe)
                  END AS code
      FROM _iobeamdb_catalog.partition_epoch pe
      WHERE pe.hypertable_name = query.hypertable_name
    ) AS code_epoch;

    IF NOT FOUND THEN
        PERFORM no_cluster_table(query);
    END IF;

    SELECT format(
      $$ SELECT *
         FROM (%s) AS union_epoch
         LIMIT %L
      $$,
          string_agg('('||inner_sql||')', ' UNION ALL '),
          query.limit_rows)
    INTO sql_code;

    RAISE NOTICE E'Cross-node SQL:\n%\n', sql_code;
    RETURN sql_code;
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
    RETURN QUERY EXECUTE format(
        $$
    SELECT row_to_json(ans)::text
    FROM (%s) as ans
    $$, ioql_exec_query_record_sql(query));
END
$BODY$
LANGUAGE plpgsql STABLE;
