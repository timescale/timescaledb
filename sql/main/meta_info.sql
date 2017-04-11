CREATE OR REPLACE FUNCTION _timescaledb_internal.get_meta_server_name()
RETURNS TEXT
LANGUAGE SQL STABLE
AS
$BODY$
    SELECT m.server_name::TEXT
    FROM _timescaledb_catalog.meta m;
$BODY$;


CREATE OR REPLACE FUNCTION _timescaledb_internal.get_meta_database_name()
RETURNS TEXT
LANGUAGE SQL STABLE
AS
$BODY$
    SELECT m.database_name::TEXT
    FROM _timescaledb_catalog.meta m;
$BODY$;
