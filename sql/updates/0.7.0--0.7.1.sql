DROP FUNCTION IF EXISTS _timescaledb_internal.create_hypertable_row(REGCLASS, NAME, NAME, NAME, NAME, INTEGER, NAME, NAME, BIGINT, NAME, REGPROC);
DROP FUNCTION IF EXISTS _timescaledb_internal.rename_hypertable(NAME, NAME, NAME, NAME);

DROP FUNCTION IF EXISTS drop_chunks(bigint,name,name,boolean);
DROP FUNCTION IF EXISTS drop_chunks(timestamptz,name,name,boolean);
