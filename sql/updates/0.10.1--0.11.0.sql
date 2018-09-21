-- Adaptive chunking
CREATE OR REPLACE FUNCTION _timescaledb_internal.calculate_chunk_interval(
        dimension_id INTEGER,
        dimension_coord BIGINT,
        chunk_target_size BIGINT
) RETURNS BIGINT AS '@MODULE_PATHNAME@', 'ts_calculate_chunk_interval' LANGUAGE C;

ALTER TABLE _timescaledb_catalog.hypertable ADD COLUMN chunk_sizing_func_schema NAME;
ALTER TABLE _timescaledb_catalog.hypertable ADD COLUMN chunk_sizing_func_name NAME;
ALTER TABLE _timescaledb_catalog.hypertable ADD COLUMN chunk_target_size BIGINT CHECK (chunk_target_size >= 0);
UPDATE _timescaledb_catalog.hypertable SET chunk_target_size = 0;
UPDATE _timescaledb_catalog.hypertable SET chunk_sizing_func_schema = '_timescaledb_internal';
UPDATE _timescaledb_catalog.hypertable SET chunk_sizing_func_name = 'calculate_chunk_interval';
ALTER TABLE _timescaledb_catalog.hypertable ALTER COLUMN chunk_target_size SET NOT NULL;
ALTER TABLE _timescaledb_catalog.hypertable ALTER COLUMN chunk_sizing_func_schema SET NOT NULL;
ALTER TABLE _timescaledb_catalog.hypertable ALTER COLUMN chunk_sizing_func_name SET NOT NULL;

DROP FUNCTION IF EXISTS create_hypertable(regclass,name,name,integer,name,name,anyelement,boolean,boolean,regproc,boolean);
DROP FUNCTION IF EXISTS _timescaledb_internal.time_to_internal(anyelement,regtype);
