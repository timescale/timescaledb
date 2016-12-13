CREATE OR REPLACE FUNCTION _meta.sync_only_insert()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    schema_name NAME;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on % table', TG_TABLE_NAME
        USING ERRCODE = 'IO101';
    END IF;

    FOR schema_name IN
    SELECT n.schema_name
    FROM public.node AS n
    LOOP
        EXECUTE format(
            $$
                INSERT INTO %I.%I SELECT $1.*
            $$,
            schema_name,
            TG_TABLE_NAME
        )
        USING NEW;
    END LOOP;
    RETURN NEW;
END
$BODY$;



