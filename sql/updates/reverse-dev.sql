
-- drop storage_policy
ALTER EXTENSION timescaledb DROP TABLE _timescaledb_catalog.storage_policy;
ALTER EXTENSION timescaledb DROP SEQUENCE _timescaledb_catalog.storage_policy_id_seq;
DROP TABLE _timescaledb_catalog.storage_policy;
