-- needed post 1.7.0 to fixup continuous aggregates created in 1.7.0 ---
DO $$
DECLARE
 vname regclass;
 altercmd text;
 ts_version TEXT;
BEGIN
    SELECT extversion INTO ts_version FROM pg_extension WHERE extname = 'timescaledb';

    FOR vname IN select format('%I.%I', user_view_schema, user_view_name)::regclass from _timescaledb_catalog.continuous_agg where materialized_only = false
    LOOP
        -- the cast from oid to text returns
        -- quote_qualified_identifier (see regclassout).
        --
        -- We use the if statement to handle pre-2.0 as well as
        -- post-2.0.  This could be turned into a procedure if we want
        -- to have something more generic, but right now it is just
        -- this case.
        IF ts_version < '2.0.0' THEN
            altercmd := format('ALTER VIEW %s SET (timescaledb.materialized_only=false) ', vname::text);
        ELSE
            altercmd := format('ALTER MATERIALIZED VIEW %s SET (timescaledb.materialized_only=false) ', vname::text);
        END IF;
        RAISE INFO 'cmd executed: %', altercmd;
        EXECUTE altercmd;
    END LOOP;
    EXCEPTION WHEN OTHERS THEN RAISE;
END
$$;

-- can only be dropped after views have been rebuilt
DROP FUNCTION IF EXISTS _timescaledb_internal.cagg_watermark(oid);

