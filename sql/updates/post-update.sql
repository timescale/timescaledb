-- needed post 1.7.0 to fixup continuous aggregates created in 1.7.0 ---
DO $$
DECLARE
 vname regclass;
 altercmd text;
BEGIN
    FOR vname IN select format('%I.%I', user_view_schema, user_view_name)::regclass from _timescaledb_catalog.continuous_agg where materialized_only = false
    LOOP
        -- the cast from oid to text returns quote_qualified_identifier
        -- (see regclassout)
        altercmd := format('ALTER VIEW %s SET (timescaledb.materialized_only=false) ', vname::text);
        RAISE INFO 'cmd executed: %', altercmd;
        EXECUTE altercmd;
    END LOOP;
    EXCEPTION WHEN OTHERS THEN RAISE;
END
$$;

-- can only be dropped after views have been rebuilt
DROP FUNCTION IF EXISTS _timescaledb_internal.cagg_watermark(oid);

