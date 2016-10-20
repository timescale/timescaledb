CREATE OR REPLACE FUNCTION create_schema(
    schema_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            CREATE SCHEMA %I
        $$, schema_name);
END
$BODY$;

CREATE OR REPLACE FUNCTION create_cluster_table(
    schema_name NAME,
    table_name  NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            CREATE TABLE IF NOT EXISTS %I.%I (
                time BIGINT NOT NULL
            )
        $$, schema_name, table_name);
END
$BODY$;

CREATE OR REPLACE FUNCTION create_cluster_distinct_table(
    schema_name NAME,
    table_name  NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            CREATE TABLE IF NOT EXISTS %1$I.%2$I (
                field TEXT,
                value TEXT,
                last_time_approx BIGINT,
                PRIMARY KEY(field, value)
            );
        $$, schema_name, table_name);
END
$BODY$;

CREATE OR REPLACE FUNCTION create_local_distinct_table(
    schema_name        NAME,
    table_name         NAME,
    cluster_table_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            CREATE TABLE IF NOT EXISTS %1$I.%2$I (PRIMARY KEY(field, value))
            INHERITS(%1$I.%3$I);

        $$, schema_name, table_name, cluster_table_name);
END
$BODY$;

CREATE OR REPLACE FUNCTION create_remote_distinct_table(
    schema_name        NAME,
    table_name         NAME,
    cluster_table_name NAME,
    server_name        NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            CREATE FOREIGN TABLE IF NOT EXISTS %1$I.%2$I () INHERITS(%1$I.%3$I) SERVER %4$I OPTIONS (schema_name '%1$I')
        $$,
        schema_name, table_name, cluster_table_name, server_name);
END
$BODY$;

CREATE OR REPLACE FUNCTION create_master_table(
    schema_name        NAME,
    table_name         NAME,
    cluster_table_name NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            CREATE TABLE IF NOT EXISTS %1$I.%2$I () INHERITS(%1$I.%3$I)
        $$,
        schema_name, table_name, cluster_table_name);
END
$BODY$;

CREATE OR REPLACE FUNCTION create_remote_table(
    schema_name        NAME,
    table_name         NAME,
    cluster_table_name NAME,
    server_name        NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            CREATE FOREIGN TABLE IF NOT EXISTS %1$I.%2$I () INHERITS(%1$I.%3$I) SERVER %4$I OPTIONS (schema_name '%1$I')
        $$,
        schema_name, table_name, cluster_table_name, server_name);
END
$BODY$;

CREATE OR REPLACE FUNCTION on_create_namespace()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    remote_node node;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on namespace table';
    END IF;

    PERFORM create_schema(NEW.schema_name);
    PERFORM create_cluster_table(NEW.schema_name, NEW.cluster_table_name);
    PERFORM create_cluster_distinct_table(NEW.schema_name, NEW.cluster_distinct_table_name);
    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';

BEGIN;
DROP TRIGGER IF EXISTS trigger_on_create_namespace
ON namespace;
CREATE TRIGGER trigger_on_create_namespace AFTER INSERT OR UPDATE OR DELETE ON namespace
FOR EACH ROW EXECUTE PROCEDURE on_create_namespace();
COMMIT;


CREATE OR REPLACE FUNCTION on_create_namespace_node()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    namespace_row namespace;
    node_row      node;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on namespace table';
    END IF;

    SELECT *
    INTO STRICT namespace_row
    FROM namespace AS ns
    WHERE ns.name = NEW.namespace_name;

    IF NEW.database_name = current_database() THEN
        PERFORM create_master_table(namespace_row.schema_name, NEW.master_table_name, namespace_row.cluster_table_name);
        PERFORM create_local_distinct_table(namespace_row.schema_name, NEW.distinct_local_table_name,
                                            namespace_row.cluster_distinct_table_name);
    ELSE
        SELECT *
        INTO STRICT node_row
        FROM node AS n
        WHERE n.database_name = NEW.database_name;

        PERFORM create_remote_table(namespace_row.schema_name, NEW.remote_table_name, namespace_row.cluster_table_name,
                                    node_row.server_name);
        PERFORM create_remote_distinct_table(namespace_row.schema_name, NEW.distinct_remote_table_name,
                                             namespace_row.cluster_distinct_table_name,
                                             node_row.server_name);
    END IF;
    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';

BEGIN;
DROP TRIGGER IF EXISTS trigger_on_create_namespace_node  ON namespace_node;
CREATE TRIGGER trigger_on_create_namespace_node AFTER INSERT OR UPDATE OR DELETE ON namespace_node
FOR EACH ROW EXECUTE PROCEDURE on_create_namespace_node();
COMMIT;
