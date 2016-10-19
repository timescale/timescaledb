DROP TYPE IF EXISTS namespace_partition_type CASCADE;
CREATE TYPE namespace_partition_type AS (project_id BIGINT, name TEXT, replica_no SMALLINT, partition_no SMALLINT, total_partitions SMALLINT);

CREATE OR REPLACE FUNCTION get_namespace(_query ioql_query)
    RETURNS namespace_type AS $$
SELECT (ROW (_query.project_id, _query.namespace_name, 1) :: namespace_type).*;
$$ LANGUAGE 'sql' STABLE;

CREATE OR REPLACE FUNCTION get_partitions_for_namespace(_ns namespace_type)
    RETURNS SETOF namespace_partition_type AS $$
SELECT DISTINCT
    project_id,
    namespace,
    replica_no,
    "partition",
    total_partitions
FROM data_tables
WHERE project_id = _ns.project_id
      AND namespace = _ns.name
      AND replica_no = _ns.replica_no
$$ LANGUAGE 'sql' STABLE;

CREATE OR REPLACE FUNCTION get_data_tables_for_partitions_time_desc(_np namespace_partition_type)
    RETURNS SETOF data_tables AS $$
SELECT dt.*
FROM data_tables dt
WHERE project_id = _np.project_id
      AND namespace = _np.name
      AND replica_no = _np.replica_no
      AND "partition" = _np.partition_no
ORDER BY GREATEST(start_time, end_time) DESC
$$ LANGUAGE 'sql' STABLE;

CREATE OR REPLACE FUNCTION ioql_query_nodes_individually(
    cluster_table REGCLASS,
    query         ioql_query,
    function_name REGPROC,
    columnDef     TEXT
)
    RETURNS SETOF RECORD AS
$BODY$
DECLARE
    --r query_return%rowtype;
    serverName     TEXT;
    connName       TEXT;
    found          BOOLEAN := FALSE;
    query_sql      TEXT;
    remote_result  INT := 0;
    remote_cnt     INT := 0;
    remote_servers TEXT [] := ARRAY [] :: TEXT [];
BEGIN
    query_sql := format('SELECT * FROM %s(%L) as res(%s)', function_name, query, columnDef);

    FOR serverName, connName IN
    SELECT
        cfs.srvname AS server_name,
        conn
    FROM
    pg_inherits i
    LEFT JOIN pg_foreign_table cft ON (cft.ftrelid = i.inhrelid)
    LEFT JOIN pg_foreign_server cfs ON (cft.ftserver = cfs.oid)
    LEFT JOIN dblink_get_connections() conn ON (cfs.srvname = ANY (conn))
    WHERE
        i.inhparent = cluster_table
    ORDER BY server_name ASC NULLS LAST LOOP

        IF serverName IS NOT NULL THEN
            IF connName IS NULL THEN
                PERFORM dblink_connect(serverName, serverName);
            END IF;

            RAISE NOTICE E'ioql remote query send node:% sql:\n%', serverName, query_sql;

            SELECT *
            FROM dblink_send_query(serverName, query_sql)
            INTO remote_result;

            IF remote_result != 1 THEN
                --can happen when an exception occured on a previous and not caught (i.e. statement timout on previous request)
                RAISE WARNING 'ioql remote warning (might be ok, gonna retry) % %', remote_result, dblink_error_message(
                    serverName);

                PERFORM dblink_disconnect(serverName);
                PERFORM dblink_connect(serverName, serverName);

                SELECT *
                FROM dblink_send_query(serverName, query_sql)
                INTO remote_result;

                IF remote_result != 1 THEN --now this really should not happen
                    RAISE EXCEPTION 'ioql remote error % %', remote_result, dblink_error_message(serverName);
                END IF;
            END IF;
            remote_servers := remote_servers || ARRAY [serverName];
        ELSE
            RAISE NOTICE E'ioql local query:\n%', query_sql;
            RETURN QUERY EXECUTE query_sql;
        END IF;
    END LOOP;

    FOR serverName IN --has to be childName not oid because we want the master table on both nodes locally, /NOT/ the remote table on local node
    SELECT unnest(remote_servers)
    LOOP
        RETURN QUERY EXECUTE format($$ SELECT * FROM dblink_get_result($1, true) AS res(%s) $$, columnDef)
        USING serverName;

        EXECUTE format($$ SELECT * FROM dblink_get_result($1, true) AS res(%s) $$, columnDef)
        USING serverName; --should return no rows. clears.

        GET DIAGNOSTICS remote_cnt := ROW_COUNT;
        IF remote_cnt != 0 THEN
            RAISE EXCEPTION 'ioql remote count error %', remote_cnt;
        END IF;
    END LOOP;

    EXCEPTION
    WHEN OTHERS THEN
        PERFORM 1
        FROM unnest(dblink_get_connections()) conn, dblink_disconnect(conn);
        RAISE;
END
$BODY$
LANGUAGE plpgsql STABLE;

CREATE OR REPLACE FUNCTION ioql_exec_query_record_sql(_query ioql_query)
    RETURNS TEXT AS $BODY$
BEGIN
    IF to_regclass(get_cluster_name(get_namespace(_query)) :: cstring) IS NULL THEN
        RETURN format($$ SELECT * FROM no_cluster_table(%L) $$, _query);
    END IF;

    IF _query.aggregate IS NULL THEN
        RETURN format(
            $$
                SELECT *
                FROM ioql_exec_query_nonagg(%L) as ans(%s)
                ORDER BY time DESC NULLS LAST
            $$, _query, get_column_def_list(_query));
    ELSE
        RETURN format($$ SELECT * FROM ioql_exec_query_agg(%L) as ans(%s) ORDER BY time DESC NULLS LAST $$,
                      _query,
                      get_finalize_aggregate_column_def(_query, _query.select_field, _query.aggregate));
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



