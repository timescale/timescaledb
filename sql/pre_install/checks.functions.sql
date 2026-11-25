-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Validation functions used in CHECK constraints of catalog tables.
-- They must exist before pre_install/tables.sql runs.
-- Violations are raised as TS601 (ERRCODE_TS_INVALID_CATALOG_VALUE).

CREATE OR REPLACE FUNCTION _timescaledb_functions.relation_validate(_relid oid, _relkinds "char"[], _label text)
RETURNS oid LANGUAGE plpgsql AS $$
DECLARE
  _relkind "char";
  _owner oid;
BEGIN
  SELECT c.relkind, c.relowner INTO _relkind, _owner FROM pg_catalog.pg_class c WHERE c.oid = _relid;

  IF NOT FOUND THEN
    RAISE EXCEPTION '% with OID % does not exist', _label, _relid USING ERRCODE = 'TS601';
  END IF;

  IF _relid < 16384 THEN
    RAISE EXCEPTION '% "%" is a system relation', _label, _relid::regclass USING ERRCODE = 'TS601';
  END IF;

  IF _relkind <> ALL(_relkinds) THEN
    RAISE EXCEPTION '% "%" is not a %', _label, _relid::regclass, CASE WHEN 'v' = ANY(_relkinds) THEN 'view' ELSE 'table' END USING ERRCODE = 'TS601';
  END IF;

  RETURN _owner;
END
$$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.owner_validate(_owner oid, _label text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF NOT pg_catalog.pg_has_role(_owner, 'USAGE') AND NOT pg_catalog.pg_has_role(_owner, 'SET') THEN
    RAISE EXCEPTION 'role "%" does not have the privileges of % "%"', current_user, _label, _owner::regrole USING ERRCODE = 'TS601';
  END IF;
END
$$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.chunk_validate(_relid regclass, _hypertable_id integer)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
  _owner oid;
  _ht_relid oid;
BEGIN
  _owner := _timescaledb_functions.relation_validate(_relid, ARRAY['r', 'f']::"char"[], 'chunk relation');

  SELECT c.oid INTO _ht_relid
  FROM _timescaledb_catalog.hypertable h
  JOIN pg_catalog.pg_namespace n ON n.nspname = h.schema_name
  JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = h.table_name
  WHERE h.id = _hypertable_id;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'hypertable with ID % of chunk relation "%" does not exist', _hypertable_id, _relid USING ERRCODE = 'TS601';
  END IF;

  IF NOT EXISTS (SELECT FROM pg_catalog.pg_inherits i WHERE i.inhrelid = _relid AND i.inhparent = _ht_relid) THEN
    RAISE EXCEPTION 'chunk relation "%" is not a child of its hypertable', _relid USING ERRCODE = 'TS601';
  END IF;

  PERFORM _timescaledb_functions.owner_validate(_owner, 'table owner');

  RETURN true;
END
$$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.compression_settings_validate(_relid regclass, _compress_relid regclass)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
  _owner oid;
  _compress_owner oid;
BEGIN
  _owner := _timescaledb_functions.relation_validate(_relid, ARRAY['r', 'f']::"char"[], 'relation');
  PERFORM _timescaledb_functions.owner_validate(_owner, 'table owner');

  IF _compress_relid IS NULL THEN
    RETURN true;
  END IF;

  _compress_owner := _timescaledb_functions.relation_validate(_compress_relid, ARRAY['r']::"char"[], 'compressed relation');

  IF _compress_relid = _relid THEN
    RAISE EXCEPTION 'compressed relation "%" is not a valid table', _compress_relid USING ERRCODE = 'TS601';
  END IF;

  IF _compress_owner <> _owner THEN
    RAISE EXCEPTION 'compressed relation "%" has a different owner than "%"', _compress_relid, _relid USING ERRCODE = 'TS601';
  END IF;

  RETURN true;
END
$$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.chunk_rewrite_validate(_chunk_relid regclass, _new_relid regclass)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
  _owner oid;
  _new_owner oid;
BEGIN
  _owner := _timescaledb_functions.relation_validate(_chunk_relid, ARRAY['r', 'f']::"char"[], 'chunk relation');

  IF NOT EXISTS (SELECT FROM _timescaledb_catalog.chunk ch WHERE ch.relid = _chunk_relid)
    AND NOT EXISTS (SELECT FROM _timescaledb_catalog.compression_settings cs WHERE cs.compress_relid = _chunk_relid)
  THEN
    RAISE EXCEPTION 'relation "%" is not a chunk', _chunk_relid USING ERRCODE = 'TS601';
  END IF;

  _new_owner := _timescaledb_functions.relation_validate(_new_relid, ARRAY['r']::"char"[], 'rewrite relation');

  IF _new_owner <> _owner THEN
    RAISE EXCEPTION 'rewrite relation "%" has a different owner than "%"', _new_relid, _chunk_relid USING ERRCODE = 'TS601';
  END IF;

  -- the rewrite relation must not be a hypertable, a chunk or a compressed chunk
  IF EXISTS (SELECT FROM pg_catalog.pg_inherits i WHERE i.inhrelid = _new_relid OR i.inhparent = _new_relid)
    OR EXISTS (SELECT FROM _timescaledb_catalog.compression_settings cs WHERE cs.relid = _new_relid OR cs.compress_relid = _new_relid)
  THEN
    RAISE EXCEPTION 'rewrite relation "%" is in use', _new_relid USING ERRCODE = 'TS601';
  END IF;

  PERFORM _timescaledb_functions.owner_validate(_owner, 'table owner');

  RETURN true;
END
$$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.hypertable_validate(_id integer, _schema_name name, _table_name name, _associated_schema_name name)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
  _relid oid;
  _owner oid;
  _nspid oid;
BEGIN
  SELECT c.oid INTO _relid
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = _schema_name AND c.relname = _table_name;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'hypertable relation "%.%" does not exist', _schema_name, _table_name USING ERRCODE = 'TS601';
  END IF;

  _owner := _timescaledb_functions.relation_validate(_relid, ARRAY['r']::"char"[], 'hypertable relation');

  IF EXISTS (
    SELECT FROM _timescaledb_catalog.chunk ch
    WHERE ch.hypertable_id = _id
      AND NOT EXISTS (SELECT FROM pg_catalog.pg_inherits i WHERE i.inhrelid = ch.relid AND i.inhparent = _relid))
  THEN
    RAISE EXCEPTION 'hypertable relation "%.%" is not the parent of its chunks', _schema_name, _table_name USING ERRCODE = 'TS601';
  END IF;

  PERFORM _timescaledb_functions.owner_validate(_owner, 'table owner');

  -- chunks are created in the associated schema without a permission check
  IF _associated_schema_name <> '_timescaledb_internal' THEN
    SELECT n.oid INTO _nspid FROM pg_catalog.pg_namespace n WHERE n.nspname = _associated_schema_name;

    IF NOT FOUND THEN
      RAISE EXCEPTION 'associated schema "%" does not exist', _associated_schema_name USING ERRCODE = 'TS601';
    END IF;

    IF NOT pg_catalog.has_schema_privilege(_nspid, 'CREATE') THEN
      RAISE EXCEPTION 'role "%" cannot create chunks in associated schema "%"', current_user, _associated_schema_name USING ERRCODE = 'TS601';
    END IF;
  END IF;

  RETURN true;
END
$$ SET search_path TO pg_catalog, pg_temp;

CREATE OR REPLACE FUNCTION _timescaledb_functions.bgw_job_validate(_owner regrole, _proc_schema name, _proc_name name, _check_schema name, _check_name name)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
  _prokind "char";
BEGIN
  IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles r WHERE r.oid = _owner) THEN
    RAISE EXCEPTION 'job owner with OID % does not exist', _owner::oid USING ERRCODE = 'TS601';
  END IF;

  PERFORM _timescaledb_functions.owner_validate(_owner, 'job owner');

  -- a missing job procedure is reported when the job runs
  SELECT p.prokind INTO _prokind
  FROM pg_catalog.pg_proc p
  JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
  WHERE n.nspname = _proc_schema AND p.proname = _proc_name
    AND pg_catalog.oidvectortypes(p.proargtypes) = 'integer, jsonb';

  IF FOUND AND _prokind NOT IN ('f', 'p') THEN
    RAISE EXCEPTION 'job procedure "%.%" is not a function or procedure', _proc_schema, _proc_name USING ERRCODE = 'TS601';
  END IF;

  IF (_check_schema IS NULL) <> (_check_name IS NULL) THEN
    RAISE EXCEPTION 'job check function needs both a schema and a name' USING ERRCODE = 'TS601';
  END IF;

  IF _check_name IS NULL THEN
    RETURN true;
  END IF;

  -- a missing check function is skipped with a warning
  SELECT p.prokind INTO _prokind
  FROM pg_catalog.pg_proc p
  JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
  WHERE n.nspname = _check_schema AND p.proname = _check_name
    AND pg_catalog.oidvectortypes(p.proargtypes) = 'jsonb';

  IF FOUND AND _prokind <> 'f' THEN
    RAISE EXCEPTION 'job check function "%.%" is not a function', _check_schema, _check_name USING ERRCODE = 'TS601';
  END IF;

  RETURN true;
END
$$ SET search_path TO pg_catalog, pg_temp;
