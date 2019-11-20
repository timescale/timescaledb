DO
$BODY$
DECLARE
    hypertable_name TEXT;
BEGIN
    SELECT first_dim.schema_name || '.' || first_dim.table_name
    FROM _timescaledb_catalog.continuous_agg ca
    INNER JOIN LATERAL (
        SELECT dim.*, h.*
        FROM _timescaledb_catalog.hypertable h
        INNER JOIN _timescaledb_catalog.dimension dim ON (dim.hypertable_id = h.id)
        WHERE ca.raw_hypertable_id = h.id
        ORDER by dim.id ASC
        LIMIT 1
    ) first_dim ON true
    WHERE first_dim.column_type IN (REGTYPE 'int2', REGTYPE 'int4', REGTYPE 'int8')
    AND (first_dim.integer_now_func_schema IS NULL OR first_dim.integer_now_func IS NULL)
    INTO hypertable_name;

    IF hypertable_name is not null AND (current_setting('timescaledb.ignore_update_errors', true) is null OR current_setting('timescaledb.ignore_update_errors', true) != 'on') THEN
        RAISE 'The continuous aggregate on hypertable "%" will break unless an integer_now func is set using set_integer_now_func().', hypertable_name;
    END IF;
END
$BODY$;
