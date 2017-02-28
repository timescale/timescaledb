CREATE OR REPLACE FUNCTION _timescaledb_internal.on_change_meta()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    cluster_user_row _timescaledb_catalog.cluster_user;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        PERFORM _timescaledb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
    END IF;

    IF NEW.database_name <> current_database() THEN
        PERFORM _timescaledb_internal.create_server(NEW.server_name, NEW.hostname, NEW.port, NEW.database_name);

        FOR cluster_user_row IN
        SELECT * FROM _timescaledb_catalog.cluster_user LOOP
            PERFORM _timescaledb_internal.create_user_mapping(cluster_user_row, NEW.server_name);
        END LOOP;
    END IF;
    RETURN NEW;
END
$BODY$;
