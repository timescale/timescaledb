CREATE OR REPLACE FUNCTION _iobeamdb_meta.on_change_node()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        PERFORM _iobeamdb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
    END IF;

    EXECUTE format(
        $$
            CREATE SCHEMA IF NOT EXISTS %I;
        $$,
        NEW.schema_name);

    IF NEW.database_name <> current_database() THEN
        PERFORM _iobeamdb_internal.create_server(NEW.server_name, NEW.hostname, NEW.database_name);

        PERFORM _iobeamdb_internal.create_user_mapping(cluster_user, NEW.server_name)
        FROM _iobeamdb_catalog.cluster_user;

        EXECUTE format(
            $$
                IMPORT FOREIGN SCHEMA _iobeamdb_catalog
                FROM SERVER %I
                INTO %I;
            $$,
            NEW.server_name, NEW.schema_name);
    END IF;
    RETURN NEW;
END
$BODY$
SET client_min_messages = WARNING --supress schema if exists notice.
;

CREATE OR REPLACE FUNCTION _iobeamdb_meta.sync_node()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    schema_name NAME;
BEGIN
    --TODO: need more complete logic for node joins
    --copy existing tables that need to be synced over (first checking those tables are empty on new node). Now we assume all joins happen at beginning
    --take list of synced tables from sync_triggers + the chunk_table.
    IF NEW.database_name <> current_database() THEN
        EXECUTE format(
            $$
                INSERT INTO %I.node SELECT * from _iobeamdb_catalog.node;
            $$,
            NEW.schema_name);
        EXECUTE format(
            $$
                INSERT INTO %I.cluster_user SELECT * from _iobeamdb_catalog.cluster_user;
            $$,
            NEW.schema_name);
        EXECUTE format(
            $$
                INSERT INTO %I.meta SELECT * from _iobeamdb_catalog.meta;
            $$,
            NEW.schema_name);
    END IF;

    FOR schema_name IN
    SELECT n.schema_name
    FROM _iobeamdb_catalog.node n
    WHERE n.schema_name <> NEW.schema_name AND
          n.database_name <> current_database()
    LOOP
        EXECUTE format(
            $$
                INSERT INTO %I.node SELECT $1.*
            $$,
            schema_name
        )
        USING NEW;
    END LOOP;

    PERFORM _iobeamdb_meta.assign_default_replica_node(NEW.database_name, h.id)
    FROM _iobeamdb_catalog.hypertable h;

    RETURN NEW;
END
$BODY$;
