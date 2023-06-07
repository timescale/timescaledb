-- API changes related to hypertable generalization
DROP FUNCTION IF EXISTS @extschema@.add_dimension(regclass,dimension_info,boolean);
DROP FUNCTION IF EXISTS @extschema@.create_hypertable(regclass,dimension_info,boolean,boolean,boolean);
DROP FUNCTION IF EXISTS @extschema@.set_partitioning_interval(regclass,anyelement,name);
DROP FUNCTION IF EXISTS @extschema@.by_hash(name,integer,regproc);
DROP FUNCTION IF EXISTS @extschema@.by_range(name,anyelement,regproc);

DROP TYPE IF EXISTS _timescaledb_internal.dimension_info CASCADE;
