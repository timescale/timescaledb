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



