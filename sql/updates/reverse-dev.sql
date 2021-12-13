DROP PROCEDURE IF EXISTS recompress_chunk;
DROP FUNCTION IF EXISTS _timescaledb_internal.chunk_status;

--undo compression feature for caggs
--check that all continuous aggregates have compression disabled
--this check is sufficient as we cannot have compressed chunks, if
-- compression is disabled
DO $$
DECLARE
  cagg_name NAME;
  matht_name NAME;
  matht_schema NAME;
BEGIN
    FOR cagg_name , matht_schema, matht_name IN
        SELECT view_name, 
               materialization_hypertable_schema ,
               materialization_hypertable_name   
        FROM timescaledb_information.continuous_aggregates
        WHERE compression_enabled is TRUE
    LOOP
        RAISE NOTICE 'compression enabled for continuous aggregate %', cagg_name 
            USING DETAIL = 'Please disable compression on all continuous aggregates before downgrading',
            HINT = ('To disable compression, call decompress_chunk( %.% ) to decompress chunks, then drop any existing compression policy on the continuous aggregate, and finally run ALTER MATERIALIZED VIEW % SET timescaledb.compress = ''false'' ', matht_schema, matht_name, cagg_name );
    END LOOP;
END $$;

-- revert changes to continuous aggregates view definition
DROP VIEW IF EXISTS timescaledb_information.continuous_aggregates;
