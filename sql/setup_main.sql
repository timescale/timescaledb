--NOTICE: UPGRADE-SCRIPT-NEEDED contents in this file are not auto-upgraded. setup_main will be redefined
--but not re-run so changes need to be included in upgrade scripts.

CREATE OR REPLACE FUNCTION _timescaledb_internal.setup_main(restore BOOLEAN = FALSE)
    RETURNS void LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    table_name NAME;
BEGIN
    -- no DELETE: it would be a no-op
    DROP TRIGGER IF EXISTS trigger_1_main_on_change_hypertable
    ON _timescaledb_catalog.hypertable;
    CREATE TRIGGER trigger_1_main_on_change_hypertable
    AFTER INSERT OR UPDATE OR DELETE ON _timescaledb_catalog.hypertable
    FOR EACH ROW EXECUTE PROCEDURE _timescaledb_internal.on_change_hypertable();

    -- No support for TRUNCATE currently, so have a trigger to prevent it on
    -- all meta tables.
    FOREACH table_name IN ARRAY ARRAY ['hypertable','dimension', 'dimension_slice',
            'chunk_constraint'] :: NAME [] LOOP
        EXECUTE format(
            $$
                DROP TRIGGER IF EXISTS trigger_block_truncate ON _timescaledb_catalog.%1$s;
                CREATE TRIGGER trigger_block_truncate
                BEFORE TRUNCATE ON _timescaledb_catalog.%1$s
                FOR EACH STATEMENT EXECUTE PROCEDURE _timescaledb_internal.on_truncate_block();
            $$, table_name);
    END LOOP;
END
$BODY$
SET client_min_messages = WARNING -- supress notices for trigger drops
;
