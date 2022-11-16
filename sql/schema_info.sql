-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- This file contains functions related to getting information about the
-- schema of a hypertable, including columns, their types, etc.


-- Check if a given table OID is a main table (i.e. the table a user
-- targets for SQL operations) for a hypertable
CREATE OR REPLACE FUNCTION _timescaledb_internal.is_main_table(
    table_oid regclass
)
    RETURNS bool LANGUAGE SQL STABLE AS
$BODY$
    SELECT EXISTS(SELECT 1 FROM _timescaledb_catalog.hypertable WHERE table_name = relname AND schema_name = nspname)
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    WHERE c.OID = table_oid;
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Check if given table is a hypertable's main table
CREATE OR REPLACE FUNCTION _timescaledb_internal.is_main_table(
    schema_name NAME,
    table_name  NAME
)
    RETURNS BOOLEAN LANGUAGE SQL STABLE AS
$BODY$
     SELECT EXISTS(
         SELECT 1 FROM _timescaledb_catalog.hypertable h
         WHERE h.schema_name = is_main_table.schema_name AND
               h.table_name = is_main_table.table_name
     );
$BODY$ SET search_path TO pg_catalog, pg_temp;

-- Get a hypertable given its main table OID
CREATE OR REPLACE FUNCTION _timescaledb_internal.hypertable_from_main_table(
    table_oid regclass
)
    RETURNS _timescaledb_catalog.hypertable LANGUAGE SQL STABLE AS
$BODY$
    SELECT h.*
    FROM pg_class c
    INNER JOIN pg_namespace n ON (n.OID = c.relnamespace)
    INNER JOIN _timescaledb_catalog.hypertable h ON (h.table_name = c.relname AND h.schema_name = n.nspname)
    WHERE c.OID = table_oid;
$BODY$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_internal.main_table_from_hypertable(
    hypertable_id int
)
    RETURNS regclass LANGUAGE SQL STABLE AS
$BODY$
    SELECT format('%I.%I',h.schema_name, h.table_name)::regclass
    FROM _timescaledb_catalog.hypertable h
    WHERE id = hypertable_id;
$BODY$ SET search_path TO pg_catalog, pg_temp;

