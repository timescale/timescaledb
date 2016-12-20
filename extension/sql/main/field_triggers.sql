/*
    This file has functions that implement changes to hypertable columns as
    change to the underlying chunk tables.
*/

CREATE OR REPLACE FUNCTION _sysinternal.create_partition_constraint_for_field(
    hypertable_name NAME,
    field_name      NAME
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    PERFORM _sysinternal.add_partition_constraint(pr.schema_name, pr.table_name, p.keyspace_start, p.keyspace_end,
                                                  p.epoch_id)
    FROM partition_epoch pe
    INNER JOIN partition p ON (p.epoch_id = pe.id)
    INNER JOIN partition_replica pr ON (pr.partition_id = p.id)
    WHERE pe.hypertable_name = create_partition_constraint_for_field.hypertable_name
          AND pe.partitioning_field = create_partition_constraint_for_field.field_name;
END
$BODY$;

-- Used to update a hypertable (or the insert temp table) when a new
-- field is added.
CREATE OR REPLACE FUNCTION _sysinternal.create_field_on_table(
    schema_name   NAME,
    table_name    NAME,
    field         NAME,
    attnum        int2,
    data_type_oid REGTYPE,
    default_value TEXT,
    not_null      BOOLEAN
)
    RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
  null_constraint TEXT := 'NOT NULL';
  default_constraint TEXT := '';
  created_columns_att_num INT2;
BEGIN
    IF NOT not_null THEN
      null_constraint = 'NULL';
    END IF;

    default_constraint = 'DEFAULT '|| default_value;

    EXECUTE format(
        $$
            ALTER TABLE %1$I.%2$I ADD COLUMN %3$I %4$s %5$s %6$s
        $$,
        schema_name, table_name, field, data_type_oid, default_constraint, null_constraint);

    SELECT att.attnum INTO STRICT created_columns_att_num
    FROM pg_attribute att
    WHERE att.attrelid = format('%I.%I', schema_name, table_name)::regclass AND att.attname = field
    AND NOT attisdropped;

    IF created_columns_att_num IS DISTINCT FROM attnum THEN
      RAISE EXCEPTION 'Inconsistent state: the attnum of newly created colum does not match (% vs %)', attnum, created_columns_att_num
      USING ERRCODE = 'IO501';
    END IF;
END
$BODY$;


CREATE OR REPLACE FUNCTION _sysinternal.drop_field_on_table(
    schema_name   NAME,
    table_name    NAME,
    field         NAME
) RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            ALTER TABLE %1$I.%2$I DROP COLUMN %3$I
        $$,
        schema_name, table_name, field);
END
$BODY$;

CREATE OR REPLACE FUNCTION _sysinternal.exec_alter_column_set_default(
    schema_name   NAME,
    table_name    NAME,
    field         NAME,
    new_default_value TEXT
) RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            ALTER TABLE %1$I.%2$I ALTER COLUMN %3$I SET DEFAULT %4$L
        $$,
        schema_name, table_name, field, new_default_value);
END
$BODY$;

CREATE OR REPLACE FUNCTION _sysinternal.exec_alter_table_rename_column(
    schema_name   NAME,
    table_name    NAME,
    old_field     NAME,
    new_field     NAME
) RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
    EXECUTE format(
        $$
            ALTER TABLE %1$I.%2$I RENAME COLUMN %3$I TO %4$I
        $$,
        schema_name, table_name, old_field, new_field);
END
$BODY$;



CREATE OR REPLACE FUNCTION _sysinternal.exec_alter_column_set_not_null(
schema_name   NAME,
table_name    NAME,
field         NAME,
new_not_null  BOOLEAN
) RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
BEGIN
IF new_not_null THEN
  EXECUTE format(
    $$
        ALTER TABLE %1$I.%2$I ALTER COLUMN %3$I SET NOT NULL
    $$,
    schema_name, table_name, field);
ELSE
  EXECUTE format(
    $$
        ALTER TABLE %1$I.%2$I ALTER COLUMN %3$I DROP NOT NULL
    $$,
    schema_name, table_name, field);
END IF;
END
$BODY$;

CREATE OR REPLACE FUNCTION _sysinternal.populate_distinct_table(
    hypertable_name  NAME,
    field         NAME
) RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
   distinct_replica_node_row  distinct_replica_node;
   chunk_replica_node_row chunk_replica_node;
BEGIN
  FOR distinct_replica_node_row IN
    SELECT *
    FROM distinct_replica_node drn
    WHERE drn.hypertable_name = populate_distinct_table.hypertable_name AND
          drn.database_name = current_database()
    LOOP
      FOR chunk_replica_node_row IN
        SELECT crn.*
        FROM chunk_replica_node crn
        INNER JOIN partition_replica pr ON (pr.id = crn.partition_replica_id)
        WHERE pr.hypertable_name = distinct_replica_node_row.hypertable_name AND
              pr.replica_id = distinct_replica_node_row.replica_id AND
              crn.database_name = current_database()
        LOOP
            EXECUTE format($$
                INSERT INTO %I.%I(field, value)
                   SELECT DISTINCT %L, %I
                   FROM %I.%I
                ON CONFLICT DO NOTHING
              $$,
              distinct_replica_node_row.schema_name, distinct_replica_node_row.table_name,
              field, field,
              chunk_replica_node_row.schema_name, chunk_replica_node_row.table_name
            );
        END LOOP;
    END LOOP;
END
$BODY$;

CREATE OR REPLACE FUNCTION _sysinternal.unpopulate_distinct_table(
    hypertable_name  NAME,
    field         NAME
) RETURNS VOID LANGUAGE PLPGSQL VOLATILE AS
$BODY$
DECLARE
   distinct_replica_node_row  distinct_replica_node;
BEGIN
  FOR distinct_replica_node_row IN
    SELECT *
    FROM distinct_replica_node drn
    WHERE drn.hypertable_name = unpopulate_distinct_table.hypertable_name AND
          drn.database_name = current_database()
    LOOP
        EXECUTE format($$
                DELETE FROM %I.%I WHERE field = %L
          $$,
          distinct_replica_node_row.schema_name, distinct_replica_node_row.table_name, field
        );
    END LOOP;
END
$BODY$;

CREATE OR REPLACE FUNCTION _sysinternal.on_modify_field()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    hypertable_row hypertable;
    update_found BOOLEAN = false;
BEGIN

    IF TG_OP = 'INSERT' THEN
      SELECT *
      INTO STRICT hypertable_row
      FROM hypertable AS h
      WHERE h.name = NEW.hypertable_name;

      -- update root table
      PERFORM _sysinternal.create_field_on_table(hypertable_row.root_schema_name, hypertable_row.root_table_name,
                                                NEW.name, NEW.attnum, NEW.data_type, NEW.default_value, NEW.not_null);
      -- update insert temp table
      PERFORM _sysinternal.create_field_on_table(hypertable_row.associated_schema_name, hypertable_row.insert_temp_table_name,
                                                NEW.name, NEW.attnum, NEW.data_type, NEW.default_value, NEW.not_null);
      -- update temp table redirect rule
      PERFORM _sysinternal.create_insert_temp_table_rule(hypertable_row.associated_schema_name, hypertable_row.insert_temp_table_name,
                                                         hypertable_row.root_schema_name, hypertable_row.root_table_name);

      IF new.created_on <> current_database() THEN
        PERFORM set_config('io.ignore_ddl_in_trigger', 'true', true);
        -- update main table on others
        PERFORM _sysinternal.create_field_on_table(hypertable_row.main_schema_name, hypertable_row.main_table_name,
                                                  NEW.name, NEW.attnum, NEW.data_type, NEW.default_value, NEW.not_null);
     END IF;

      PERFORM _sysinternal.create_partition_constraint_for_field(NEW.hypertable_name, NEW.name);
      RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
      SELECT *
      INTO STRICT hypertable_row
      FROM hypertable AS h
      WHERE h.name = NEW.hypertable_name;

      IF NEW.default_value IS DISTINCT FROM OLD.default_value THEN
        update_found = TRUE;
        -- update root table
        PERFORM _sysinternal.exec_alter_column_set_default(hypertable_row.root_schema_name, hypertable_row.root_table_name,
                                              NEW.name, NEW.default_value);
        -- update insert temp table
        PERFORM _sysinternal.exec_alter_column_set_default(hypertable_row.associated_schema_name, hypertable_row.insert_temp_table_name,
                                              NEW.name, NEW.default_value);
        -- update temp table redirect rule
        PERFORM _sysinternal.create_insert_temp_table_rule(hypertable_row.associated_schema_name, hypertable_row.insert_temp_table_name,
                                                           hypertable_row.root_schema_name, hypertable_row.root_table_name);

        IF NEW.modified_on <> current_database() THEN
          PERFORM set_config('io.ignore_ddl_in_trigger', 'true', true);
          -- update main table on others
          PERFORM _sysinternal.exec_alter_column_set_default(hypertable_row.main_schema_name, hypertable_row.main_table_name,
                                              NEW.name, NEW.default_value);
        END IF;
      END IF;
      IF NEW.not_null IS DISTINCT FROM OLD.not_null THEN
        update_found = TRUE;
        -- update root table
        PERFORM _sysinternal.exec_alter_column_set_not_null(hypertable_row.root_schema_name, hypertable_row.root_table_name,
                                              NEW.name, NEW.not_null);
        -- update insert temp table
        PERFORM _sysinternal.exec_alter_column_set_not_null(hypertable_row.associated_schema_name, hypertable_row.insert_temp_table_name,
                                              NEW.name, NEW.not_null);
        -- update temp table redirect rule
        PERFORM _sysinternal.create_insert_temp_table_rule(hypertable_row.associated_schema_name, hypertable_row.insert_temp_table_name,
                                                           hypertable_row.root_schema_name, hypertable_row.root_table_name);
        IF NEW.modified_on <> current_database() THEN
          PERFORM set_config('io.ignore_ddl_in_trigger', 'true', true);
          -- update main table on others
          PERFORM _sysinternal.exec_alter_column_set_not_null(hypertable_row.main_schema_name, hypertable_row.main_table_name,
                                              NEW.name, NEW.not_null);
        END IF;
      END IF;
      IF NEW.name IS DISTINCT FROM OLD.name THEN
        update_found = TRUE;
        -- update root table
        PERFORM _sysinternal.exec_alter_table_rename_column(hypertable_row.root_schema_name, hypertable_row.root_table_name,
                                              OLD.name, NEW.name);
        -- update insert temp table
        PERFORM _sysinternal.exec_alter_table_rename_column(hypertable_row.associated_schema_name, hypertable_row.insert_temp_table_name,
                                              OLD.name, NEW.name);
        -- update temp table redirect rule
        PERFORM _sysinternal.create_insert_temp_table_rule(hypertable_row.associated_schema_name, hypertable_row.insert_temp_table_name,
                                                           hypertable_row.root_schema_name, hypertable_row.root_table_name);
        IF NEW.modified_on <> current_database() THEN
          PERFORM set_config('io.ignore_ddl_in_trigger', 'true', true);
          -- update main table on others
          PERFORM _sysinternal.exec_alter_table_rename_column(hypertable_row.main_schema_name, hypertable_row.main_table_name,
                                              OLD.name, NEW.name);
        END IF;
      END IF;

      IF NEW.is_distinct IS DISTINCT FROM OLD.is_distinct THEN
          update_found = TRUE;
          IF NEW.is_distinct THEN
            PERFORM  _sysinternal.populate_distinct_table(NEW.hypertable_name, NEW.name);
          ELSE
            PERFORM  _sysinternal.unpopulate_distinct_table(NEW.hypertable_name, NEW.name);
          END IF;
      END IF;

      IF NOT update_found THEN
        RAISE EXCEPTION 'Invalid update type on %', TG_TABLE_NAME
        USING ERRCODE = 'IO101';
      END IF;
      RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
      --handled by deleted log
      RETURN OLD;
    END IF;
END
$BODY$
SET SEARCH_PATH = 'public';

CREATE OR REPLACE FUNCTION _sysinternal.on_deleted_field()
    RETURNS TRIGGER LANGUAGE PLPGSQL AS
$BODY$
DECLARE
    hypertable_row hypertable;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'Only inserts supported on % table', TG_TABLE_NAME
        USING ERRCODE = 'IO101';
    END IF;

    SELECT *
    INTO STRICT hypertable_row
    FROM hypertable AS h
    WHERE h.name = NEW.hypertable_name;

    -- drop temp table redirect rule briefly
    PERFORM _sysinternal.drop_insert_temp_table_rule(hypertable_row.root_schema_name, hypertable_row.root_table_name);
    -- update root table
    PERFORM _sysinternal.drop_field_on_table(hypertable_row.root_schema_name, hypertable_row.root_table_name,
                                              NEW.name);
    -- update insert temp table
    PERFORM _sysinternal.drop_field_on_table(hypertable_row.associated_schema_name, hypertable_row.insert_temp_table_name,
                                              NEW.name);
    -- update temp table redirect rule
    PERFORM _sysinternal.create_insert_temp_table_rule(hypertable_row.associated_schema_name, hypertable_row.insert_temp_table_name,
                                                       hypertable_row.root_schema_name, hypertable_row.root_table_name);

    IF NEW.deleted_on <> current_database() THEN
      PERFORM set_config('io.ignore_ddl_in_trigger', 'true', true);
      -- update main table on others
      PERFORM _sysinternal.drop_field_on_table(hypertable_row.main_schema_name, hypertable_row.main_table_name,
                                              NEW.name);
    END IF;

    RETURN NEW;
END
$BODY$
SET SEARCH_PATH = 'public';
