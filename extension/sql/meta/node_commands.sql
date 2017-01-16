--This file provides a utility for executing dblink commands transactionally (almost) with the local transaction.

--Called by _meta.node_transaction_exec_with_return to start a transaction. Can be called directly to start a transaction 
--if you need to use some of the more custom dblink functions. Returns the dblink connection name for the started transaction. 
CREATE OR REPLACE FUNCTION _meta.node_transaction_start(database_name NAME, server_name NAME)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    conn_exists BOOLEAN;
    conn_name TEXT;
BEGIN
    IF database_name = current_database() THEN 
        RETURN NULL;
    END IF;
    
    conn_name = 'conn_'|| server_name;

    SELECT conn_name = ANY (conn) INTO conn_exists
    FROM dblink_get_connections() conn;

    IF conn_exists IS NULL OR NOT conn_exists THEN
        --tells c code to commit in precommit.
        PERFORM dblink_connect(conn_name, server_name);
        PERFORM dblink_exec(conn_name, 'BEGIN');
        PERFORM _sysinternal.register_dblink_precommit_connection(conn_name);
    END IF;

    RETURN conn_name;
END
$BODY$;

--This should be called to execute code on a meta node with a text return. It is not necessary to
--call _sysinternal.meta_transaction_start() beforehand. The code excuted by this function
--will be automatically committed when the local transaction commits (in pre-commit).
CREATE OR REPLACE FUNCTION  _meta.node_transaction_exec_with_return(database_name NAME, server_name NAME, sql_code text)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    conn_name TEXT;
    return_value TEXT;
BEGIN
    SELECT _meta.node_transaction_start(database_name, server_name) INTO conn_name;
    
    IF conn_name IS NOT NULL THEN
        SELECT t.r INTO return_value FROM dblink(conn_name, sql_code) AS t(r TEXT);
    ELSE
        EXECUTE sql_code INTO return_value;
    END IF;
    RETURN return_value;
END
$BODY$;


