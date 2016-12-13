CREATE OR REPLACE FUNCTION _meta.on_create_node()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on node table'
        USING ERRCODE = 'IO101';
    END IF;
    EXECUTE format(
        $$
            CREATE SCHEMA IF NOT EXISTS %I;
        $$,
        NEW.schema_name);

    PERFORM _sysinternal.create_server(NEW.server_name, NEW.hostname, NEW.database_name);

    PERFORM _sysinternal.create_user_mapping(cluster_user, NEW.server_name)
    FROM cluster_user;

    EXECUTE format(
        $$
            IMPORT FOREIGN SCHEMA public
            FROM SERVER %I
            INTO %I;
        $$,
        NEW.server_name, NEW.schema_name);
    RETURN NEW;
END
$BODY$;

CREATE OR REPLACE FUNCTION _meta.sync_node()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    schema_name NAME;
BEGIN
    --TODO: need more complete logic for node joins
    --copy existing tables that need to be synced over (first checking those tables are empty on new node). Now we assume all joins happen at beginning
    --take list of synced tables from sync_triggers + the chunk_table.
    EXECUTE format(
        $$
            INSERT INTO %I.node SELECT * from public.node;
        $$,
        NEW.schema_name);
    EXECUTE format(
        $$
            INSERT INTO %I.cluster_user SELECT * from cluster_user;
        $$,
        NEW.schema_name);
    EXECUTE format(
        $$
            INSERT INTO %I.meta SELECT * from meta;
        $$,
        NEW.schema_name);

    FOR schema_name IN
    SELECT n.schema_name
    FROM node AS n
    WHERE n.schema_name <> NEW.schema_name
    LOOP
        EXECUTE format(
            $$
                INSERT INTO %I.node SELECT $1.*
            $$,
            schema_name
        )
        USING NEW;
    END LOOP;
    RETURN NEW;
END
$BODY$;
