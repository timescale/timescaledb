CREATE OR REPLACE FUNCTION _iobeamdb_internal.on_create_cluster_user()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    node_row _iobeamdb_catalog.node;
    meta_row _iobeamdb_catalog.meta;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on node table'
        USING ERRCODE = 'IO101';
    END IF;

    --NOTE:  creating the role should be done outside this purview. Permissions are complex and should be set by the DBA
    -- before creating the cluster user

    FOR node_row IN SELECT *
                    FROM _iobeamdb_catalog.node
                    WHERE database_name <> current_database() LOOP
        PERFORM _iobeamdb_internal.create_user_mapping(NEW, node_row.server_name);
    END LOOP;
    RETURN NEW;

    FOR meta_row IN SELECT *
                    FROM _iobeamdb_catalog.meta LOOP
        PERFORM _iobeamdb_internal.create_user_mapping(NEW, meta_row.server_name);
    END LOOP;
    RETURN NEW;

END
$BODY$
SET SEARCH_PATH = 'public';
