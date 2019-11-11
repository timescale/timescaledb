--rewrite hypertable catalog table because previous updates messed up
--and the table is now using the missingval optimization which doesnt work
--with catalog scans; Note this is equivalent to a VACUUM FULL
--but that command cannot be used inside an update script.
CLUSTER  _timescaledb_catalog.hypertable USING hypertable_pkey;
ALTER TABLE _timescaledb_catalog.hypertable SET WITHOUT CLUSTER;

--The metadata table also has the missingval optimization;
CLUSTER  _timescaledb_catalog.metadata USING metadata_pkey;
ALTER TABLE _timescaledb_catalog.metadata SET WITHOUT CLUSTER;