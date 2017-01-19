CREATE OR REPLACE FUNCTION get_meta_server_name()
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    server_name  TEXT;
BEGIN
    SELECT m.server_name
    INTO STRICT server_name
    FROM meta m;

    RETURN server_name;
END
$BODY$;


CREATE OR REPLACE FUNCTION get_meta_database_name()
    RETURNS TEXT LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
    database_name  TEXT;
BEGIN
    SELECT m.database_name
    INTO STRICT database_name
    FROM meta m;

    RETURN database_name;
END
$BODY$;



