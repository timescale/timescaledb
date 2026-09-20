DROP FUNCTION IF EXISTS _timescaledb_functions.move_to_columnstore(REGCLASS, BOOLEAN);
DELETE FROM _timescaledb_catalog.compression_algorithm WHERE id = 8 AND version = 1 AND name = 'COMPRESSION_ALGORITHM_AIC';
