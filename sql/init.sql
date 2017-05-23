-- This is executed whenever the extension is created
DO $$
DECLARE 
    do_setup BOOLEAN;
BEGIN 
    SELECT current_setting('timescaledb.restoring', true) IS DISTINCT FROM 'on' INTO do_setup;

    IF do_setup THEN
        PERFORM _timescaledb_internal.setup_main();
    END IF;
END
$$;
