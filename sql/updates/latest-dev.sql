DROP FUNCTION IF EXISTS @extschema@.add_retention_policy(REGCLASS, "any", BOOL);

DROP FUNCTION IF EXISTS @extschema@.add_compression_policy(REGCLASS, "any", BOOL);
DROP FUNCTION IF EXISTS @extschema@.detach_data_node;
DROP FUNCTION IF EXISTS @extschema@.remove_continuous_aggregate_policy(REGCLASS, BOOL);
