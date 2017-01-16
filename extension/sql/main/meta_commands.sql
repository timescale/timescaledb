--This file provides a utility for executing dblink commands transactionally (almost) with the local transaction.
--Any command executed by the _sysinternal.meta_transaction_exec command will be committed on the meta node only
--when the local transaction commits.

--Called by _sysinternal.meta_transaction_exec to start a transaction. Can be called directly to start a transaction
--if you need to use some of the more custom dblink functions. Returns the dblink connection name for the started transaction.
CREATE OR REPLACE FUNCTION _sysinternal.meta_transaction_start()
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    conn_exists BOOLEAN;
    conn_name TEXT = 'meta_conn';
BEGIN
    SELECT conn_name = ANY (conn) INTO conn_exists
    FROM dblink_get_connections() conn;

    IF conn_exists IS NULL OR NOT conn_exists THEN
        --tells c code to commit in precommit.
        PERFORM dblink_connect(conn_name, get_meta_server_name());
        PERFORM dblink_exec(conn_name, 'BEGIN');
        PERFORM _sysinternal.register_dblink_precommit_connection(conn_name);
    END IF;

    RETURN conn_name;
END
$BODY$;

--This should be called to execute code on a meta node. It is not necessary to
--call _sysinternal.meta_transaction_start() beforehand. The code excuted by this function
--will be automatically committed when the local transaction commits (in pre-commit).
CREATE OR REPLACE FUNCTION  _sysinternal.meta_transaction_exec(sql_code text)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    conn_name TEXT;
BEGIN
    IF get_meta_database_name() <> current_database() THEN
        SELECT _sysinternal.meta_transaction_start() INTO conn_name;
        PERFORM * FROM dblink(conn_name, sql_code) AS t(r TEXT);
    ELSE
        EXECUTE sql_code;
    END IF;
END
$BODY$;

--This should be called to execute code on a meta node with a text return. It is not necessary to
--call _sysinternal.meta_transaction_start() beforehand. The code excuted by this function
--will be automatically committed when the local transaction commits (in pre-commit).
CREATE OR REPLACE FUNCTION  _sysinternal.meta_transaction_exec_with_return(sql_code text)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    conn_name TEXT;
    return_value TEXT;
BEGIN
    IF get_meta_database_name() <> current_database() THEN
        SELECT _sysinternal.meta_transaction_start() INTO conn_name;
        SELECT t.r INTO return_value FROM dblink(conn_name, sql_code) AS t(r TEXT);
    ELSE
        EXECUTE sql_code INTO return_value;
    END IF;
    RETURN return_value;
END
$BODY$;


--This should be called to execute code on a meta node in a way that doesn't wait for the local commit.
--i.e. This command is committed on meta right away w/o waiting for the local commit.
--A consequence is that the remote command will not be rolled back if the local transaction is.
CREATE OR REPLACE FUNCTION  _sysinternal.meta_immediate_commit_exec_with_return(sql_code text)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    return_value TEXT;
BEGIN
    IF get_meta_database_name() <> current_database() THEN
        SELECT t.r INTO return_value FROM dblink(get_meta_server_name(), sql_code) AS t(r TEXT);
    ELSE
        EXECUTE sql_code INTO return_value;
    END IF;
    RETURN return_value;
END
$BODY$;

