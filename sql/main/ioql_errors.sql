CREATE OR REPLACE FUNCTION no_cluster_table(query ioql_query)
    RETURNS TABLE(json TEXT) LANGUAGE PLPGSQL STABLE AS
$BODY$
BEGIN
    RAISE EXCEPTION 'Namespace ''%'' does not exist', query.namespace_name
    USING ERRCODE = 'IO001';
END
$BODY$;