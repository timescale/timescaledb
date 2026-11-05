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

-- The ids of the enabled policies a new table with this name matches, ranked in the order of
-- precedence: 1 is the policy it would get. This is the lookup CREATE TABLE does, and everything
-- that shows or compares policies is built on it.
CREATE OR REPLACE FUNCTION _timescaledb_functions.storage_policy_matches(schema_name NAME, table_name NAME)
RETURNS TABLE (rank INTEGER, id INTEGER)
AS '@MODULE_PATHNAME@', 'ts_storage_policy_matches'
LANGUAGE C STABLE STRICT;

-- Warn when the policy called policy_name overlaps with a policy of the other kind. A name an
-- exact policy covers is never given to a regex policy, so an exact policy always wins over a
-- regex one. This is easy to miss when either is added, so both directions are reported. Two
-- regex policies are not compared, as whether two patterns overlap cannot be decided from the
-- patterns alone. The priority settles those. A disabled policy applies to nothing, so it neither
-- warns nor is warned about.
CREATE OR REPLACE FUNCTION _timescaledb_functions.storage_policy_warn_overlaps(policy_name NAME)
RETURNS VOID
LANGUAGE plpgsql STABLE SET search_path TO pg_catalog, pg_temp AS
$$
DECLARE
    this _timescaledb_catalog.storage_policy;
    other RECORD;
BEGIN
    SELECT * INTO this FROM _timescaledb_catalog.storage_policy p
    WHERE p.name = policy_name AND p.enabled;
    IF NOT FOUND THEN
        RETURN;
    END IF;

    IF this.match_kind = 'regex' THEN
        -- the exact policies this one is also a match for
        FOR other IN
            SELECT p.name, p.schema_name, p.table_name
            FROM _timescaledb_catalog.storage_policy p
            WHERE p.match_kind = 'exact' AND p.enabled
              AND EXISTS (SELECT FROM _timescaledb_functions.storage_policy_matches(
                                         p.schema_name, p.table_name) m
                          WHERE m.id = this.id)
            ORDER BY p.schema_name, p.table_name
        LOOP
            RAISE WARNING 'storage policy "%" also matches "%"."%", which has its own policy "%"',
                this.name, other.schema_name, other.table_name, other.name
                USING DETAIL = format('The exact policy wins, so "%s" does not apply to that table.',
                                      this.name);
        END LOOP;
    ELSE
        -- the regex policies that also match this name
        FOR other IN
            SELECT p.name, p.priority
            FROM _timescaledb_functions.storage_policy_matches(this.schema_name, this.table_name) m
            JOIN _timescaledb_catalog.storage_policy p ON p.id = m.id
            WHERE p.match_kind = 'regex'
            ORDER BY m.rank
        LOOP
            RAISE WARNING 'storage policy "%" for "%"."%" takes precedence over the regex policy "%" (priority %)',
                this.name, this.schema_name, this.table_name, other.name, other.priority
                USING DETAIL = format('New tables with this name get "%s", not "%s".',
                                      this.name, other.name);
        END LOOP;
    END IF;
END
$$;

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

    PERFORM _timescaledb_functions.storage_policy_warn_overlaps(name);

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

    -- only a change of what the policy matches, or switching it on, can create an overlap
    IF schema_name IS NOT NULL OR table_name IS NOT NULL OR enabled IS TRUE THEN
        PERFORM _timescaledb_functions.storage_policy_warn_overlaps(name);
    END IF;
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

-- Every enabled policy a new table named schema_name.table_name matches, in the order of
-- precedence: rank 1 is the policy it would get, and each next rank is the one that would apply
-- if the ranks before it were removed or disabled. An exact policy comes before any regex
-- policy, and the regex policies follow in order of priority, the lowest value first. The
-- match_kind says why a policy is where it is, and the priority is NULL for an exact one.
CREATE OR REPLACE FUNCTION @extschema@.storage_policy_match(schema_name NAME, table_name NAME)
RETURNS TABLE (rank INTEGER, name NAME, match_kind TEXT, priority INTEGER, policy JSONB)
LANGUAGE sql STABLE SET search_path TO pg_catalog, pg_temp AS
$$
    SELECT m.rank, p.name, p.match_kind, p.priority, p.policy
    -- the parameters are qualified, as the columns of p have the same names
    FROM _timescaledb_functions.storage_policy_matches(storage_policy_match.schema_name,
                                                       storage_policy_match.table_name) m
    JOIN _timescaledb_catalog.storage_policy p ON p.id = m.id
    ORDER BY m.rank
$$;

-- What CREATE TABLE schema_name.table_name WITH (options) would do under the current storage
-- policies, one row per setting and where it comes from: "policy" or "statement". The options
-- are the statement's tsdb.* options as a JSON object, for example '{"chunk_interval": "1 day"}',
-- and win over the policy as they do in CREATE TABLE. Nothing is created.
CREATE OR REPLACE FUNCTION @extschema@.storage_policy_explain(
    schema_name NAME, table_name NAME, options JSONB DEFAULT NULL)
RETURNS TABLE (setting TEXT, value TEXT, source TEXT)
AS '@MODULE_PATHNAME@', 'ts_storage_policy_explain'
LANGUAGE C STABLE;
