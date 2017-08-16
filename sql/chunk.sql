CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_calculate_default_range_open(
        dimension_value   BIGINT,
        interval_length   BIGINT,
    OUT range_start       BIGINT,
    OUT range_end         BIGINT)
    AS '$libdir/timescaledb', 'dimension_calculate_open_range_default' LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_calculate_default_range_closed(
        dimension_value   BIGINT,
        num_slices        SMALLINT,
    OUT range_start       BIGINT,
    OUT range_end         BIGINT)
    AS '$libdir/timescaledb', 'dimension_calculate_closed_range_default' LANGUAGE C STABLE;


-- Trigger for when chunk rows are changed.
-- On Insert: create chunk table, add indexes, add triggers.
-- On Delete: drop table
CREATE OR REPLACE FUNCTION _timescaledb_internal.on_change_chunk()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    kind                  pg_class.relkind%type;
    hypertable_row        _timescaledb_catalog.hypertable;
    main_table_oid        OID;
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM _timescaledb_internal.chunk_create_table(NEW.id);

        PERFORM _timescaledb_internal.create_chunk_index_row(NEW.schema_name, NEW.table_name,
                                hi.main_schema_name, hi.main_index_name, hi.definition)
        FROM _timescaledb_catalog.hypertable_index hi
        WHERE hi.hypertable_id = NEW.hypertable_id
        ORDER BY format('%I.%I',main_schema_name, main_index_name)::regclass;

        SELECT * INTO STRICT hypertable_row FROM _timescaledb_catalog.hypertable WHERE id = NEW.hypertable_id;
        main_table_oid := format('%I.%I', hypertable_row.schema_name, hypertable_row.table_name)::regclass;

        PERFORM _timescaledb_internal.create_chunk_constraint(NEW.id, oid)
        FROM pg_constraint
        WHERE conrelid = main_table_oid
        AND _timescaledb_internal.need_chunk_constraint(NEW.hypertable_id, oid);

        PERFORM _timescaledb_internal.create_chunk_trigger(NEW.id, tgname,
            _timescaledb_internal.get_general_trigger_definition(oid))
        FROM pg_trigger
        WHERE tgrelid = main_table_oid
        AND _timescaledb_internal.need_chunk_trigger(NEW.hypertable_id, oid);

        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        -- when deleting the chunk row from the metadata table,
        -- also DROP the actual chunk table that holds data.
        -- Note that the table could already be deleted in case this
        -- trigger fires as a result of a DROP TABLE on the hypertable
        -- that this chunk belongs to.

        EXECUTE format(
                $$
                SELECT c.relkind FROM pg_class c WHERE relname = '%I' AND relnamespace = '%I'::regnamespace
                $$, OLD.table_name, OLD.schema_name
        ) INTO kind;

        IF kind IS NULL THEN
            RETURN OLD;
        END IF;

        EXECUTE format(
            $$
            DROP TABLE %I.%I
            $$, OLD.schema_name, OLD.table_name
        );
        RETURN OLD;
    END IF;

    PERFORM _timescaledb_internal.on_trigger_error(TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME);
END
$BODY$;
