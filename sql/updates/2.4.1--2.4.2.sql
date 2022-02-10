DROP FUNCTION IF EXISTS _timescaledb_internal.time_col_name_for_chunk(name,name);
DROP FUNCTION IF EXISTS _timescaledb_internal.time_col_type_for_chunk(name,name);

-- Handle column renames for continuous aggregates that were not
-- handled correctly and fix it in the update. We save the information
-- in a table.
CREATE TABLE _timescaledb_internal.rename_tables (
       user_view regclass,
       new_name text,
       old_name text,
       partial_view regclass,
       direct_view regclass,
       mat_table regclass,
       hypertable_id int
);

-- Compare the user view and the direct view of each continuous
-- aggregate to figure out what columns that were renamed on the user
-- view but which did not propagate to the other objects of the
-- continuous aggregate since this did not work in previous versions.
WITH
  objs AS (
        SELECT format('%I.%I', user_view_schema, user_view_name)::regclass AS user_view,
               format('%I.%I', direct_view_schema, direct_view_name)::regclass AS direct_view,
               format('%I.%I', partial_view_schema, partial_view_name)::regclass AS partial_view,
               format('%I.%I', schema_name, table_name)::regclass AS mat_table,
               mat_hypertable_id AS mat_id
          FROM _timescaledb_catalog.continuous_agg
          JOIN _timescaledb_catalog.hypertable ON mat_hypertable_id = id),
  user_view AS (
        SELECT attrelid, attname, attnum, mat_id
          FROM objs, pg_attribute
         WHERE attrelid = objs.user_view),
  direct_view AS (
        SELECT attrelid, attname, attnum, mat_id
          FROM objs, pg_attribute
         WHERE attrelid = objs.direct_view)
INSERT INTO _timescaledb_internal.rename_tables
SELECT (SELECT user_view FROM objs WHERE uv.attrelid = user_view),
       uv.attname AS new_name,
       dv.attname AS old_name,
       (SELECT partial_view FROM objs WHERE uv.attrelid = user_view),
       (SELECT direct_view FROM objs WHERE uv.attrelid = user_view),
       (SELECT mat_table FROM objs WHERE uv.attrelid = user_view),
       (SELECT mat_id FROM objs WHERE uv.attrelid = user_view)
  FROM user_view uv JOIN direct_view dv USING (mat_id, attnum)
 WHERE uv.attname != dv.attname;

CREATE PROCEDURE _timescaledb_internal.alter_table_column(cagg regclass, relation regclass, old_column_name name, new_column_name name) AS $$
BEGIN
    EXECUTE format('ALTER TABLE %s RENAME COLUMN %I TO %I', relation, old_column_name, new_column_name);
END;
$$ LANGUAGE plpgsql;

-- Rename the columns for all the associated objects for continuous
-- aggregates that have renamed columns. Also rename the column in the
-- dimension table.
DO
$$
DECLARE
    user_view regclass;
    new_name name;
    old_name name;
    partial_view regclass;
    direct_view regclass;
    mat_table regclass;
    ht_id int;
BEGIN
  FOR user_view, new_name, old_name, partial_view, direct_view, mat_table, ht_id IN
  SELECT * FROM _timescaledb_internal.rename_tables
  LOOP
    -- There is no RENAME COLUMN for views, but we can use ALTER TABLE
    -- to rename a column in a view.
    CALL _timescaledb_internal.alter_table_column(user_view, partial_view, old_name, new_name);
    CALL _timescaledb_internal.alter_table_column(user_view, direct_view, old_name, new_name);
    CALL _timescaledb_internal.alter_table_column(user_view, mat_table, old_name, new_name);
    UPDATE _timescaledb_catalog.dimension SET column_name = new_name
     WHERE hypertable_id = ht_id AND column_name = old_name;
  END LOOP;
END
$$;

DROP PROCEDURE _timescaledb_internal.alter_table_column;
DROP TABLE _timescaledb_internal.rename_tables;
