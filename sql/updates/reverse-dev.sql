DROP FUNCTION IF EXISTS  @extschema@.add_retention_policy(REGCLASS, "any", BOOL, INTERVAL);
CREATE FUNCTION @extschema@.add_retention_policy(relation REGCLASS, drop_after "any", if_not_exists BOOL = false)
RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_policy_retention_add' LANGUAGE C VOLATILE STRICT;

DROP FUNCTION IF EXISTS  @extschema@.add_compression_policy(REGCLASS, "any", BOOL, INTERVAL);
CREATE FUNCTION @extschema@.add_compression_policy(hypertable REGCLASS, compress_after "any", if_not_exists BOOL = false)
RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_policy_compression_add' LANGUAGE C VOLATILE STRICT;

DROP FUNCTION IF EXISTS @extschema@.detach_data_node;
CREATE FUNCTION @extschema@.detach_data_node(
    node_name              NAME,
    hypertable             REGCLASS = NULL,
    if_attached            BOOLEAN = FALSE,
    force                  BOOLEAN = FALSE,
    repartition            BOOLEAN = TRUE
) RETURNS INTEGER
AS '@MODULE_PATHNAME@', 'ts_data_node_detach' LANGUAGE C VOLATILE;

DROP FUNCTION _timescaledb_internal.attach_osm_table_chunk( hypertable REGCLASS, chunk REGCLASS);
DROP FUNCTION _timescaledb_internal.alter_job_set_hypertable_id( job_id INTEGER, hypertable REGCLASS );
DROP FUNCTION _timescaledb_internal.unfreeze_chunk( chunk REGCLASS);
-- Drop dimension partition metadata table
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.dimension_partition;
DROP TABLE IF EXISTS _timescaledb_catalog.dimension_partition;
DROP FUNCTION IF EXISTS timescaledb_experimental.add_policies;
DROP FUNCTION IF EXISTS timescaledb_experimental.remove_policies;
DROP FUNCTION IF EXISTS timescaledb_experimental.remove_all_policies;
DROP FUNCTION IF EXISTS timescaledb_experimental.alter_policies;
DROP FUNCTION IF EXISTS timescaledb_experimental.show_policies;
DROP FUNCTION IF EXISTS @extschema@.remove_continuous_aggregate_policy(REGCLASS, BOOL, BOOL);
CREATE FUNCTION @extschema@.remove_continuous_aggregate_policy(continuous_aggregate REGCLASS, if_not_exists BOOL = false)
RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_policy_refresh_cagg_remove'
LANGUAGE C VOLATILE STRICT;

DROP VIEW IF EXISTS timescaledb_experimental.policies;

