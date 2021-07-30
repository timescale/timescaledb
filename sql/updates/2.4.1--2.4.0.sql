-- Note that 2.4.0 used CREATE VIEW instead of CREATE OR REPLACE VIEW to
-- create this view. This should be part of downgrade to 2.4.0
DROP VIEW IF EXISTS timescaledb_experimental.chunk_replication_status;
