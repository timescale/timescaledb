CREATE OR REPLACE FUNCTION _iobeamdb_internal.get_meta_server_name()
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    server_name  TEXT;
    port         INT;
BEGIN
    SELECT m.server_name, m.port
    INTO STRICT server_name, port
    FROM _iobeamdb_catalog.meta m;

    RETURN server_name;
END
$BODY$;


CREATE OR REPLACE FUNCTION _iobeamdb_internal.get_meta_database_name()
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    database_name  TEXT;
BEGIN
    SELECT m.database_name
    INTO STRICT database_name
    FROM _iobeamdb_catalog.meta m;

    RETURN database_name;
END
$BODY$;
