CREATE OR REPLACE FUNCTION _timescaledb_meta.assign_default_replica_node(
  database_name NAME,
  hypertable_id INTEGER
)
    RETURNS VOID LANGUAGE SQL VOLATILE AS
$BODY$
INSERT INTO _timescaledb_catalog.default_replica_node (database_name, hypertable_id, replica_id)
SELECT assign_default_replica_node.database_name, hr.hypertable_id, hr.replica_id
FROM _timescaledb_catalog.hypertable_replica hr
WHERE hr.hypertable_id = assign_default_replica_node.hypertable_id
ORDER BY RANDOM()
LIMIT 1
$BODY$;
