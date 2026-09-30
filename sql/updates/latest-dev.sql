
insert into _timescaledb_catalog.compression_algorithm( id, version, name, description) values
( 8, 1, 'COMPRESSION_ALGORITHM_AIC', 'aic');

DROP FUNCTION IF EXISTS _timescaledb_functions.hypertable_get_tenant_tracking_info(REGCLASS);
