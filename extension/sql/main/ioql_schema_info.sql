CREATE OR REPLACE FUNCTION get_partition_replicas(
    epoch      _iobeamdb_catalog.partition_epoch,
    replica_id SMALLINT
)
    RETURNS SETOF _iobeamdb_catalog.partition_replica LANGUAGE SQL STABLE AS
$BODY$
SELECT DISTINCT pr.*
FROM _iobeamdb_catalog.partition p
INNER JOIN _iobeamdb_catalog.partition_replica pr ON (pr.partition_id = p.id)
INNER JOIN _iobeamdb_catalog.chunk_replica_node crn ON (crn.partition_replica_id = pr.id)
WHERE p.epoch_id = epoch.id AND
      pr.replica_id = get_partition_replicas.replica_id AND
      crn.database_name = current_database()
$BODY$;

CREATE OR REPLACE FUNCTION get_local_chunk_replica_node_for_pr_time_desc(
    pr _iobeamdb_catalog.partition_replica
)
    RETURNS SETOF _iobeamdb_catalog.chunk_replica_node LANGUAGE SQL STABLE AS
$BODY$
SELECT crn.*
FROM _iobeamdb_catalog.chunk_replica_node crn
INNER JOIN _iobeamdb_catalog.chunk c ON (c.id = crn.chunk_id)
WHERE
    crn.partition_replica_id = pr.id AND
    crn.database_name = current_database()
ORDER BY GREATEST(start_time, end_time) DESC
$BODY$;
