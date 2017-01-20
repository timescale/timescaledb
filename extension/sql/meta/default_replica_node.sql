CREATE OR REPLACE FUNCTION _meta.assign_default_replica_node(
  database_name NAME,
  hypertable_name NAME
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
INSERT INTO _iobeamdb_catalog.default_replica_node (database_name, hypertable_name, replica_id)
SELECT assign_default_replica_node.database_name, hr.hypertable_name, hr.replica_id
FROM _iobeamdb_catalog.hypertable_replica hr
WHERE hr.hypertable_name = assign_default_replica_node.hypertable_name
ORDER BY RANDOM()
LIMIT 1
ON CONFLICT DO NOTHING;
$BODY$;
