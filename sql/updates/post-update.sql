-- needed post 1.7.0 to fixup continuous aggregates created in 1.7.0 ---
DO $$
<<ts_realtime_agg_update>>
DECLARE
 vname regclass;
 altercmd text;
BEGIN
    FOR vname IN select view_name from timescaledb_information.continuous_aggregates where materialized_only = false
    LOOP
        -- the cast from oid to text returns quote_qualified_identifier
        -- (see regclassout)
        altercmd := format('ALTER VIEW %s SET (timescaledb.materialized_only=false) ', vname::text) ; 
        RAISE INFO 'cmd executed: %', altercmd;
        EXECUTE altercmd;
    END LOOP;
    EXCEPTION WHEN OTHERS THEN RAISE;
END ts_realtime_agg_update $$;

