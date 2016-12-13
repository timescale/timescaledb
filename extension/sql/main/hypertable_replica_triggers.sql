CREATE OR REPLACE FUNCTION _sysinternal.on_create_hypertable_replica()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    hypertable_row hypertable;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on hypertable_replica table'
        USING ERRCODE = 'IO101';
    END IF;

    SELECT *
    INTO STRICT hypertable_row
    FROM hypertable AS h
    WHERE h.name = NEW.hypertable_name;


    PERFORM _sysinternal.create_replica_table(NEW.schema_name, NEW.table_name, hypertable_row.root_schema_name,
                                              hypertable_row.root_table_name);
    PERFORM _sysinternal.create_replica_table(NEW.distinct_schema_name, NEW.distinct_table_name,
                                              hypertable_row.distinct_schema_name, hypertable_row.distinct_table_name);

    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';
