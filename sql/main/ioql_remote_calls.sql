CREATE OR REPLACE FUNCTION ioql_exec_query_nodes(
    query         ioql_query,
    epoch         partition_epoch,
    columnDef     TEXT
)
    RETURNS SETOF RECORD AS
$BODY$
DECLARE
    database_names NAME[];
    query_replica_id SMALLINT;
    --r query_return%rowtype;
    serverName     TEXT;
    isConnected    BOOLEAN;
    query_sql      TEXT;
    remote_result  INT := 0;
    remote_cnt     INT := 0;
    remote_servers TEXT [] := ARRAY [] :: TEXT [];
BEGIN
    SELECT hr.replica_id
    INTO STRICT query_replica_id
    FROM hypertable_replica hr
    WHERE hr.hypertable_name = query.namespace_name
    ORDER BY random()
    LIMIT 1;

    SELECT ARRAY(
      SELECT DISTINCT crn.database_name
      FROM partition_replica pr 
      INNER JOIN chunk_replica_node crn ON (crn.partition_replica_id = pr.id)
      WHERE pr.hypertable_name = query.namespace_name AND
            pr.replica_id = query_replica_id
    )
    INTO database_names;

    query_sql := format('SELECT * FROM  ioql_exec_local_node(%L, %L, %L) AS res_local(%s)', query, epoch, query_replica_id, columnDef);

    FOR serverName, isConnected IN
    SELECT n.server_name, conn IS NOT NULL 
    FROM  node n 
    LEFT JOIN dblink_get_connections() conn ON (n.server_name = ANY (conn))
    WHERE
        n.database_name = ANY(database_names) AND 
        n.database_name <> current_database() 
        LOOP

        IF NOT isConnected THEN
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
                RAISE EXCEPTION 'ioql remote error % %', remote_result, dblink_error_message(serverName)
                USING ERRCODE = 'IO502';
            END IF;
        END IF;
        remote_servers := remote_servers || ARRAY [serverName];
    END LOOP;

    IF current_database() = ANY(database_names) THEN
        RAISE NOTICE E'ioql local query:\n%', query_sql;
        RETURN QUERY EXECUTE query_sql;
    END IF;

    FOR serverName IN 
    SELECT unnest(remote_servers)
    LOOP
        RETURN QUERY EXECUTE format($$ SELECT * FROM dblink_get_result(%L, true) AS res(%s) $$, serverName, columnDef);

        --should return no rows. clears.
        EXECUTE format($$ SELECT * FROM dblink_get_result(%L, true) AS res(%s) $$, serverName, columnDef);

        GET DIAGNOSTICS remote_cnt := ROW_COUNT;
        IF remote_cnt != 0 THEN
            RAISE EXCEPTION 'ioql remote count error %', remote_cnt
            USING ERRCODE = 'IO502';
        END IF;
    END LOOP;

    EXCEPTION
    WHEN OTHERS THEN
        PERFORM dblink_disconnect(conn)
        FROM unnest(dblink_get_connections()) conn;
        RAISE;
END
$BODY$
LANGUAGE plpgsql STABLE;

