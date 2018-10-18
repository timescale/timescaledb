
GRANT USAGE ON SCHEMA _timescaledb_cache TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA _timescaledb_catalog TO PUBLIC;
GRANT SELECT ON TABLE _timescaledb_internal.bgw_job_stat TO PUBLIC;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA _timescaledb_catalog TO PUBLIC;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA _timescaledb_config TO PUBLIC;

DROP FUNCTION IF EXISTS create_hypertable(regclass,name,name,integer,name,name,anyelement,boolean,boolean,regproc,boolean,text,regproc);
DROP FUNCTION IF EXISTS add_dimension(regclass,name,integer,anyelement,regproc,boolean);

