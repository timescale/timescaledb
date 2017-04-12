--This file provides a utility for executing remote commands transactionally (almost) with the local transaction.
--Most command executed by the *_exec functions will be committed only when the local transaction commits.

--Called by _timescaledb_internal.meta_transaction_exec to start a transaction. Should not be called directly.
--Returns the connection name for the started transaction.
CREATE OR REPLACE FUNCTION _timescaledb_internal.meta_transaction_start()
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    conn_exists BOOLEAN;
    conn_name TEXT = 'meta_conn';
BEGIN
    PERFORM _timescaledb_internal.clustering_not_supported();
END
$BODY$;

--This should be called to execute code on a meta node. It is not necessary to
--call _timescaledb_internal.meta_transaction_start() beforehand. The code excuted by this function
--will be automatically committed when the local transaction commits (in pre-commit).
CREATE OR REPLACE FUNCTION _timescaledb_internal.meta_transaction_exec(
    sql_code text
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    conn_name TEXT;
BEGIN
    IF _timescaledb_internal.get_meta_database_name() <> current_database() THEN
        PERFORM _timescaledb_internal.clustering_not_supported();
    ELSE
        EXECUTE sql_code;
    END IF;
END
$BODY$;

--This should be called to execute code on a meta node with a text return. It is not necessary to
--call _timescaledb_internal.meta_transaction_start() beforehand. The code excuted by this function
--will be automatically committed when the local transaction commits (in pre-commit).
CREATE OR REPLACE FUNCTION _timescaledb_internal.meta_transaction_exec_with_return(
    sql_code text
)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    conn_name TEXT;
    return_value TEXT;
BEGIN
    IF _timescaledb_internal.get_meta_database_name() <> current_database() THEN
        PERFORM _timescaledb_internal.clustering_not_supported();
    ELSE
        EXECUTE sql_code INTO return_value;
    END IF;
    RETURN return_value;
END
$BODY$;


--This should be called to execute code on a meta node in a way that doesn't wait for the local commit.
--i.e. This command is committed on meta right away w/o waiting for the local commit.
--A consequence is that the remote command will not be rolled back if the local transaction is.
CREATE OR REPLACE FUNCTION _timescaledb_internal.meta_immediate_commit_exec_with_return(
    sql_code text
)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    return_value TEXT;
BEGIN
    IF _timescaledb_internal.get_meta_database_name() <> current_database() THEN
        PERFORM _timescaledb_internal.clustering_not_supported();
    ELSE
        EXECUTE sql_code INTO return_value;
    END IF;
    RETURN return_value;
END
$BODY$;

--Called by _timescaledb_meta.node_transaction_exec_with_return to start a transaction. Should not be called directly.
--Returns the connection name for the started transaction.
CREATE OR REPLACE FUNCTION _timescaledb_meta.node_transaction_start(database_name NAME, server_name NAME)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    conn_exists BOOLEAN;
    conn_name TEXT;
BEGIN
    IF database_name = current_database() THEN
        RETURN NULL;
    END IF;

    PERFORM _timescaledb_internal.clustering_not_supported();
END
$BODY$;

--This should be called to execute code on a meta node with a text return. It is not necessary to
--call _timescaledb_internal.node_transaction_start() beforehand. The code excuted by this function
--will be automatically committed when the local transaction commits (in pre-commit).
CREATE OR REPLACE FUNCTION _timescaledb_meta.node_transaction_exec_with_return(
    database_name NAME,
    server_name   NAME,
    sql_code      TEXT
)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    conn_name TEXT;
    return_value TEXT;
BEGIN
    SELECT _timescaledb_meta.node_transaction_start(database_name, server_name) INTO conn_name;

    IF conn_name IS NOT NULL THEN
        PERFORM _timescaledb_internal.clustering_not_supported();
    ELSE
        EXECUTE sql_code INTO return_value;
    END IF;
    RETURN return_value;
END
$BODY$;

--This should be called to execute code on a data node in a way that doesn't wait for the local commit.
--i.e. This command is committed on meta right away w/o waiting for the local commit.
--A consequence is that the remote command will not be rolled back if the local transaction is.
CREATE OR REPLACE FUNCTION  _timescaledb_internal.node_immediate_commit_exec_with_return(
    database_name NAME,
    server_name   NAME,
    sql_code      TEXT
)
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    return_value TEXT;
BEGIN
    IF database_name <> current_database() THEN
        PERFORM _timescaledb_internal.clustering_not_supported();
    ELSE
        EXECUTE sql_code INTO return_value;
    END IF;
    RETURN return_value;
END
$BODY$;
