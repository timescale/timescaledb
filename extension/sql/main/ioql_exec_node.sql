CREATE OR REPLACE FUNCTION ioql_exec_local_node(query ioql_query, epoch partition_epoch, replica_id SMALLINT)
    --need replica since local node needs to know which tables its responsible for.
    RETURNS SETOF RECORD LANGUAGE PLPGSQL STABLE AS
$BODY$
DECLARE
    code TEXT;
BEGIN
    IF NOT query.aggregate IS NULL THEN
        code := ioql_query_local_node_agg_grouped_sql(query, epoch, replica_id);
    ELSE
        code := ioql_query_local_node_nonagg_sql(query, epoch, replica_id);
    END IF;
    RAISE NOTICE E'Per-Node SQL %:\n %', current_database(), code;
    RETURN QUERY EXECUTE code;
END;
$BODY$
SET constraint_exclusion = ON;
