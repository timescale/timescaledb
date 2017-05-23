-- Checks that the partition's tablespace exists
CREATE OR REPLACE FUNCTION _timescaledb_internal.on_change_partition()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.tablespace IS NOT NULL THEN
            DECLARE
                tablespace_row  pg_catalog.pg_tablespace;
            BEGIN
                SELECT * FROM pg_catalog.pg_tablespace t
                INTO tablespace_row
                WHERE t.spcname = NEW.tablespace;

                IF NOT FOUND THEN
                    RAISE EXCEPTION 'No tablespace % in database %', NEW.tablespace, current_database()
                    USING ERRCODE = 'IO501';
                END IF;
            END;
        END IF;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;

    PERFORM _timescaledb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$
SET SEARCH_PATH = 'public';
