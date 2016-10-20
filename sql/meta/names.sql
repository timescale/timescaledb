CREATE OR REPLACE FUNCTION get_cluster_table_name(
    namespace_name NAME
)
    RETURNS NAME LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT 'cluster' :: NAME;
$BODY$;

CREATE OR REPLACE FUNCTION get_master_table_name(
    namespace_name NAME
)
    RETURNS NAME LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT 'master' :: NAME;
$BODY$;

CREATE OR REPLACE FUNCTION get_remote_table_name(
    namespace_name NAME,
    remote_node    node
)
    RETURNS NAME LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT format('remote_%s', remote_node.database_name) :: NAME;
$BODY$;

CREATE OR REPLACE FUNCTION get_schema_name(
    namespace NAME
)
    RETURNS NAME LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT namespace;
$BODY$;

CREATE OR REPLACE FUNCTION get_cluster_distinct_table_name(
    namespace_name NAME
)
    RETURNS NAME LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT 'distinct' :: NAME;
$BODY$;

CREATE OR REPLACE FUNCTION get_local_distinct_table_name(
    namespace_name NAME
)
    RETURNS NAME LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT 'local_distinct' :: NAME;
$BODY$;

CREATE OR REPLACE FUNCTION get_remote_distinct_table_name(
    namespace_name NAME,
    remote_node    node
)
    RETURNS NAME LANGUAGE SQL IMMUTABLE AS
$BODY$
SELECT format('remote_distinct_%s', remote_node.database_name) :: NAME;
$BODY$;