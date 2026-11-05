-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Validate a storage policy document. Raises an error if the document is not understood.
CREATE OR REPLACE FUNCTION _timescaledb_functions.storage_policy_validate(policy JSONB)
RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_storage_policy_validate'
LANGUAGE C IMMUTABLE STRICT;

-- Only the database owner or a superuser may manage storage policies, as a policy decides
-- how other roles' new tables are stored. The check is done in C, as the functions below are
-- SECURITY DEFINER and, inside them, current_user is the function owner and not the caller.
CREATE OR REPLACE FUNCTION _timescaledb_functions.storage_policy_check_owner()
RETURNS VOID AS '@MODULE_PATHNAME@', 'ts_storage_policy_check_owner'
LANGUAGE C STABLE STRICT;

CREATE OR REPLACE FUNCTION @extschema@.add_storage_policy(
    name NAME,
    schema_name TEXT,
    table_name TEXT,
    policy JSONB,
    match_kind TEXT = 'exact',
    priority INTEGER = NULL,
    enabled BOOL = true,
    if_not_exists BOOL = false
) RETURNS INTEGER
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path TO pg_catalog, pg_temp AS
$$
DECLARE
    new_id INTEGER;
BEGIN
    PERFORM _timescaledb_functions.storage_policy_check_owner();

    IF match_kind NOT IN ('exact', 'regex') THEN
        RAISE EXCEPTION 'invalid match_kind "%"', match_kind
            USING ERRCODE = 'invalid_parameter_value', HINT = 'Valid values are "exact" and "regex".';
    END IF;
    IF match_kind = 'exact' AND priority IS NOT NULL THEN
        RAISE EXCEPTION 'priority must be NULL for exact matches'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF match_kind = 'regex' AND priority IS NULL THEN
        RAISE EXCEPTION 'priority is required for regex matches'
            USING ERRCODE = 'invalid_parameter_value',
                  HINT = 'The policy with the lowest priority value wins when several match.';
    END IF;
    IF match_kind = 'regex' THEN
        -- Reject a pattern that does not compile now, not when the first table is created. The
        -- patterns are anchored when they are used, so they are checked the same way.
        PERFORM '' ~ ('^(?:' || schema_name || ')$');
        PERFORM '' ~ ('^(?:' || table_name || ')$');
    END IF;

    PERFORM _timescaledb_functions.storage_policy_validate(policy);

    IF if_not_exists AND EXISTS (SELECT FROM _timescaledb_catalog.storage_policy p
                                 WHERE p.name = add_storage_policy.name) THEN
        RAISE NOTICE 'storage policy "%" already exists, skipping', name;
        RETURN NULL;
    END IF;

    INSERT INTO _timescaledb_catalog.storage_policy
        (name, schema_name, table_name, match_kind, priority, policy, enabled)
    VALUES (name, schema_name, table_name, match_kind, priority, policy, enabled)
    RETURNING id INTO new_id;

    RETURN new_id;
END
$$;

CREATE OR REPLACE FUNCTION @extschema@.alter_storage_policy(
    name NAME,
    schema_name TEXT = NULL,
    table_name TEXT = NULL,
    policy JSONB = NULL,
    priority INTEGER = NULL,
    enabled BOOL = NULL
) RETURNS VOID
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path TO pg_catalog, pg_temp AS
$$
DECLARE
    current_kind TEXT;
BEGIN
    PERFORM _timescaledb_functions.storage_policy_check_owner();

    SELECT p.match_kind INTO current_kind
    FROM _timescaledb_catalog.storage_policy p
    WHERE p.name = alter_storage_policy.name
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'storage policy "%" does not exist', name
            USING ERRCODE = 'undefined_object';
    END IF;

    IF priority IS NOT NULL AND current_kind <> 'regex' THEN
        RAISE EXCEPTION 'priority must be NULL for exact matches'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF current_kind = 'regex' THEN
        IF schema_name IS NOT NULL THEN
            PERFORM '' ~ ('^(?:' || schema_name || ')$');
        END IF;
        IF table_name IS NOT NULL THEN
            PERFORM '' ~ ('^(?:' || table_name || ')$');
        END IF;
    END IF;
    IF policy IS NOT NULL THEN
        PERFORM _timescaledb_functions.storage_policy_validate(policy);
    END IF;

    UPDATE _timescaledb_catalog.storage_policy p
    SET schema_name = COALESCE(alter_storage_policy.schema_name, p.schema_name),
        table_name = COALESCE(alter_storage_policy.table_name, p.table_name),
        policy = COALESCE(alter_storage_policy.policy, p.policy),
        priority = COALESCE(alter_storage_policy.priority, p.priority),
        enabled = COALESCE(alter_storage_policy.enabled, p.enabled),
        updated_at = now(),
        updated_by = current_user
    WHERE p.name = alter_storage_policy.name;
END
$$;

CREATE OR REPLACE FUNCTION @extschema@.remove_storage_policy(
    name NAME,
    if_exists BOOL = false
) RETURNS VOID
LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path TO pg_catalog, pg_temp AS
$$
BEGIN
    PERFORM _timescaledb_functions.storage_policy_check_owner();

    DELETE FROM _timescaledb_catalog.storage_policy p
    WHERE p.name = remove_storage_policy.name;

    IF NOT FOUND THEN
        IF if_exists THEN
            RAISE NOTICE 'storage policy "%" does not exist, skipping', name;
        ELSE
            RAISE EXCEPTION 'storage policy "%" does not exist', name
                USING ERRCODE = 'undefined_object';
        END IF;
    END IF;
END
$$;

-- The id of the policy a new table with this name would get, or NULL if there is none. This is
-- the lookup CREATE TABLE does, so the answer is the same as what a new table would get.
CREATE OR REPLACE FUNCTION _timescaledb_functions.storage_policy_match_id(schema_name NAME, table_name NAME)
RETURNS INTEGER AS '@MODULE_PATHNAME@', 'ts_storage_policy_match_id'
LANGUAGE C STABLE STRICT;

-- Which policy a new table named schema_name.table_name would get, and why: the match_kind is
-- "exact", or "regex" together with the priority that made it the winner.
CREATE OR REPLACE FUNCTION @extschema@.storage_policy_match(schema_name NAME, table_name NAME)
RETURNS TABLE (name NAME, match_kind TEXT, priority INTEGER, policy JSONB)
LANGUAGE sql STABLE SET search_path TO pg_catalog, pg_temp AS
$$
    SELECT p.name, p.match_kind, p.priority, p.policy
    FROM _timescaledb_catalog.storage_policy p
    -- the parameters are qualified, as the columns of p have the same names
    WHERE p.id = _timescaledb_functions.storage_policy_match_id(storage_policy_match.schema_name,
                                                                storage_policy_match.table_name)
$$;
