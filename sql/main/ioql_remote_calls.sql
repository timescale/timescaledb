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