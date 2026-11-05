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

-- The role that called a function below, which is what updated_by records. It is the role the
-- session runs as, SET ROLE included, which is also the one storage_policy_check_owner() checks.
CREATE OR REPLACE FUNCTION _timescaledb_functions.storage_policy_caller()
RETURNS NAME AS '@MODULE_PATHNAME@', 'ts_storage_policy_caller'
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
        -- and on its own, as a pattern like 'a)|(?:' only compiles inside the wrapper and then
        -- matches everything. Anchors of its own are harmless, ^(?:^a$)$ is still a.
        PERFORM '' ~ schema_name;
        PERFORM '' ~ table_name;
    END IF;

    PERFORM _timescaledb_functions.storage_policy_validate(policy);

    IF if_not_exists AND EXISTS (SELECT FROM _timescaledb_catalog.storage_policy p
                                 WHERE p.name = add_storage_policy.name) THEN
        RAISE NOTICE 'storage policy "%" already exists, skipping', name;
        RETURN NULL;
    END IF;

    INSERT INTO _timescaledb_catalog.storage_policy
        (name, schema_name, table_name, match_kind, priority, policy, enabled, updated_by)
    VALUES (name, schema_name, table_name, match_kind, priority, policy, enabled,
            _timescaledb_functions.storage_policy_caller())
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
            PERFORM '' ~ schema_name;
        END IF;
        IF table_name IS NOT NULL THEN
            PERFORM '' ~ ('^(?:' || table_name || ')$');
            PERFORM '' ~ table_name;
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
        updated_by = _timescaledb_functions.storage_policy_caller()
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
CREATE OR REPLACE FUNCTION @extschema@.show_storage_policy_matches(schema_name NAME, table_name NAME)
RETURNS TABLE (rank INTEGER, name NAME, match_kind TEXT, priority INTEGER, policy JSONB)
LANGUAGE sql STABLE SET search_path TO pg_catalog, pg_temp AS
$$
    SELECT m.rank, p.name, p.match_kind, p.priority, p.policy
    -- the parameters are qualified, as the columns of p have the same names
    FROM _timescaledb_functions.storage_policy_matches(show_storage_policy_matches.schema_name,
                                                       show_storage_policy_matches.table_name) m
    JOIN _timescaledb_catalog.storage_policy p ON p.id = m.id
    ORDER BY m.rank
$$;

-- What CREATE TABLE schema_name.table_name WITH (options) would do under the current storage
-- policies, one row per setting and where it comes from: "policy" or "statement". The options
-- are the statement's tsdb.* options as a JSON object, for example '{"chunk_interval": "1 day"}',
-- and win over the policy as they do in CREATE TABLE. Nothing is created.
CREATE OR REPLACE FUNCTION @extschema@.explain_storage_policy(
    schema_name NAME, table_name NAME, options JSONB DEFAULT NULL)
RETURNS TABLE (setting TEXT, value TEXT, source TEXT)
AS '@MODULE_PATHNAME@', 'ts_storage_policy_explain'
LANGUAGE C STABLE;

-- explain_storage_policy() as drift and apply ask for it: the setting
-- timescaledb.enable_storage_policies is for the tables a session creates, so it does not hide the
-- policy from a check or an apply that was asked for by name.
CREATE OR REPLACE FUNCTION _timescaledb_functions.storage_policy_explain(
    schema_name NAME, table_name NAME, options JSONB, ignore_guc BOOL)
RETURNS TABLE (setting TEXT, value TEXT, source TEXT)
AS '@MODULE_PATHNAME@', 'ts_storage_policy_explain'
LANGUAGE C STABLE;

-- The value of one setting in the rows explain gives, as a JSON array, or NULL if it has none.
CREATE OR REPLACE FUNCTION _timescaledb_functions.storage_policy_setting(explained JSONB, setting TEXT)
RETURNS TEXT
LANGUAGE sql IMMUTABLE PARALLEL SAFE SET search_path TO pg_catalog, pg_temp AS
$$
    SELECT x->>'value' FROM jsonb_array_elements(explained) x
    WHERE x->>'setting' = storage_policy_setting.setting
$$;

-- A column list as the hypertable reports it and as a policy spells it differ in ways that are not
-- a difference: the case of what is not quoted, the spaces around the commas, an ASC the table
-- does not keep because it is the default, and the NULLS clause that is the default of the
-- direction (NULLS LAST for ASC, NULLS FIRST for DESC).
CREATE OR REPLACE FUNCTION _timescaledb_functions.storage_policy_normalize_columns(cols TEXT)
RETURNS TEXT
LANGUAGE sql IMMUTABLE PARALLEL SAFE SET search_path TO pg_catalog, pg_temp AS
$$
    -- a quoted identifier keeps its case, and only what is outside the quotes is folded
    WITH pieces AS (
        SELECT m[1] AS piece, n
        FROM regexp_matches(cols, '"[^"]*"|[^"]+', 'g') WITH ORDINALITY AS t(m, n)
    ), folded AS (
        SELECT string_agg(CASE WHEN left(piece, 1) = '"' THEN piece ELSE lower(piece) END, '' ORDER BY n) AS s
        FROM pieces
    )
    SELECT btrim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
        s, '\s+asc\y', '', 'g'),
        '\s+desc\s+nulls\s+first\y', ' desc', 'g'),
        '(?<!desc)\s+nulls\s+last\y', '', 'g'),
        '\s*,\s*', ',', 'g'),
        '\s+', ' ', 'g'))
    FROM folded
$$;

-- Where existing tables no longer match the storage policy that applies to them today.
-- Policies are applied once, when a table is created, so a table keeps the settings it was
-- created with when the policy is edited, when someone changes the table by hand, or when it was
-- created with its own options. This compares what explain_storage_policy() says a new table with
-- the same name would get against the hypertable, and returns one row per setting that differs.
-- Only what the policy declares is compared, so a setting the policy leaves out is never drift,
-- and a table no policy matches is not listed. The settings compared are the partition
-- column, the chunk interval, the columnstore segmentby and orderby, and the ages of the
-- compression and retention policies.
--
-- A policy that is changed while this runs may give anomalous results, as the policy is read more
-- than once and no lock keeps it still.
--
-- With include_plain => true a plain table that a policy matches is listed too, with one row,
-- tsdb.hypertable, expected true and actual false. Such a table was created before the policy or
-- with tsdb.hypertable = false, and the second is a choice that this cannot tell from the first,
-- so it is off by default. Temporary tables, partitions, tables that inherit or are typed and
-- tables of an extension never get a policy and are not listed.
--
-- With no arguments every hypertable is analysed, with a schema only the hypertables in it, and
-- with a schema and a table just that one. Pass only_drift => false to get every setting that was
-- compared, with in_sync saying whether it matches, to see how a table stands against its policy.
CREATE OR REPLACE FUNCTION @extschema@.check_storage_policy_drift(
    schema_name NAME DEFAULT NULL, table_name NAME DEFAULT NULL, only_drift BOOL DEFAULT true,
    include_plain BOOL DEFAULT false)
RETURNS TABLE (hypertable_schema NAME, hypertable_name NAME, policy_name NAME,
               setting TEXT, expected TEXT, actual TEXT, in_sync BOOL)
LANGUAGE sql STABLE SET search_path TO pg_catalog, pg_temp AS
$$
    WITH hts AS (
        SELECT h.hypertable_schema AS s, h.hypertable_name AS t
        FROM timescaledb_information.hypertables h
        -- the parameters are qualified, as the function's output columns have similar names
        WHERE (check_storage_policy_drift.schema_name IS NULL
               OR h.hypertable_schema = check_storage_policy_drift.schema_name)
          AND (check_storage_policy_drift.table_name IS NULL
               OR h.hypertable_name = check_storage_policy_drift.table_name)
    ), plain AS (
        SELECT n.nspname AS s, c.relname AS t
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE check_storage_policy_drift.include_plain
          AND c.relkind = 'r' AND c.relpersistence <> 't' AND NOT c.relispartition
          AND c.reloftype = 0
          AND left(n.nspname, 12) <> '_timescaledb'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND (check_storage_policy_drift.schema_name IS NULL
               OR n.nspname = check_storage_policy_drift.schema_name)
          AND (check_storage_policy_drift.table_name IS NULL
               OR c.relname = check_storage_policy_drift.table_name)
          AND NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid)
          AND NOT EXISTS (SELECT 1 FROM pg_depend d
                          WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid
                            AND d.deptype = 'e')
          AND NOT EXISTS (SELECT 1 FROM timescaledb_information.hypertables h
                          WHERE h.hypertable_schema = n.nspname AND h.hypertable_name = c.relname)
    ), plain_matched AS (
        SELECT p.s, p.t, e.value AS policy_name
        FROM plain p
        CROSS JOIN LATERAL _timescaledb_functions.storage_policy_explain(p.s, p.t, NULL, true) e
        -- no match is a row with no value
        WHERE e.setting = 'storage_policy' AND e.value IS NOT NULL
    ), dims AS MATERIALIZED (
        -- the views are read once, and not once for every setting of every table
        SELECT d.hypertable_schema AS s, d.hypertable_name AS t, d.column_name, d.time_interval,
               d.integer_interval
        FROM timescaledb_information.dimensions d
        WHERE d.dimension_number = 1
          AND (check_storage_policy_drift.schema_name IS NULL
               OR d.hypertable_schema = check_storage_policy_drift.schema_name)
          AND (check_storage_policy_drift.table_name IS NULL
               OR d.hypertable_name = check_storage_policy_drift.table_name)
    ), cs AS MATERIALIZED (
        SELECT c.hypertable, c.segmentby, c.orderby
        FROM timescaledb_information.hypertable_columnstore_settings c
        WHERE c.hypertable IN (SELECT to_regclass(format('%I.%I', hts.s, hts.t)) FROM hts)
    ), jobs AS MATERIALIZED (
        SELECT j.hypertable_schema AS s, j.hypertable_name AS t, j.proc_name, j.config
        FROM timescaledb_information.jobs j
        WHERE j.proc_name IN ('policy_compression', 'policy_retention')
          AND (check_storage_policy_drift.schema_name IS NULL
               OR j.hypertable_schema = check_storage_policy_drift.schema_name)
          AND (check_storage_policy_drift.table_name IS NULL
               OR j.hypertable_name = check_storage_policy_drift.table_name)
    ), actual AS MATERIALIZED (
        SELECT hts.s, hts.t, d.column_name, d.time_interval, d.integer_interval,
               cs.segmentby, cs.orderby, cj.config AS cconfig, rj.config AS rconfig
        FROM hts
        LEFT JOIN dims d ON d.s = hts.s AND d.t = hts.t
        LEFT JOIN cs ON cs.hypertable = to_regclass(format('%I.%I', hts.s, hts.t))
        LEFT JOIN jobs cj ON cj.s = hts.s AND cj.t = hts.t AND cj.proc_name = 'policy_compression'
        LEFT JOIN jobs rj ON rj.s = hts.s AND rj.t = hts.t AND rj.proc_name = 'policy_retention'
    ), want AS (
        -- one row per setting of the policy, with what the table has beside it
        SELECT a.s, a.t, a.column_name, a.time_interval, a.integer_interval, a.segmentby, a.orderby,
               a.cconfig, a.rconfig, e.setting, e.value AS expected, e.source,
               max(e.value) FILTER (WHERE e.setting = 'storage_policy')
                   OVER (PARTITION BY a.s, a.t) AS policy_name
        FROM actual a
        CROSS JOIN LATERAL _timescaledb_functions.storage_policy_explain(a.s, a.t, NULL, true) e
    ), compared AS (
        SELECT a.s, a.t, a.policy_name, a.setting, a.expected, a.source,
        CASE a.setting
            WHEN 'tsdb.partition_column' THEN a.column_name::text
            WHEN 'tsdb.chunk_interval' THEN coalesce(a.time_interval::text, a.integer_interval::text)
            WHEN 'tsdb.segmentby' THEN a.segmentby
            WHEN 'tsdb.orderby' THEN a.orderby
            WHEN 'tsdb.compress_after' THEN a.cconfig->>'compress_after'
            WHEN 'retention_after' THEN a.rconfig->>'drop_after'
        END AS actual,
        CASE a.setting
            WHEN 'tsdb.partition_column' THEN a.column_name::text IS NOT DISTINCT FROM a.expected
            -- an integer time column has an integer chunk interval, not an interval
            WHEN 'tsdb.chunk_interval' THEN
                CASE WHEN a.integer_interval IS NOT NULL
                     THEN a.integer_interval::text IS NOT DISTINCT FROM btrim(a.expected)
                     ELSE a.time_interval IS NOT DISTINCT FROM a.expected::interval
                END
            -- the lists are compared without the spaces and the case
            WHEN 'tsdb.segmentby' THEN
                _timescaledb_functions.storage_policy_normalize_columns(a.segmentby)
                IS NOT DISTINCT FROM _timescaledb_functions.storage_policy_normalize_columns(a.expected)
            -- a list that leaves out the time column gets it added at the end, descending
            WHEN 'tsdb.orderby' THEN
                _timescaledb_functions.storage_policy_normalize_columns(a.orderby)
                IS NOT DISTINCT FROM _timescaledb_functions.storage_policy_normalize_columns(a.expected)
                OR _timescaledb_functions.storage_policy_normalize_columns(a.orderby)
                IS NOT DISTINCT FROM _timescaledb_functions.storage_policy_normalize_columns(
                       a.expected || ', ' || quote_ident(a.column_name) || ' DESC')
            -- NEVER means there is no job
            WHEN 'tsdb.compress_after' THEN
                CASE WHEN a.expected = 'never' THEN a.cconfig IS NULL
                     ELSE (a.cconfig->>'compress_after')::interval IS NOT DISTINCT FROM a.expected::interval
                END
            -- NEVER means there is no job
            WHEN 'retention_after' THEN
                CASE WHEN a.expected = 'never' THEN a.rconfig IS NULL
                     ELSE (a.rconfig->>'drop_after')::interval IS NOT DISTINCT FROM a.expected::interval
                END
        END AS same
        FROM want a
        WHERE a.source = 'policy' AND a.policy_name IS NOT NULL
          AND a.setting IN ('tsdb.partition_column', 'tsdb.chunk_interval', 'tsdb.segmentby',
                            'tsdb.orderby', 'tsdb.compress_after', 'retention_after')
    )
    SELECT r.* FROM (
        SELECT c.s, c.t, c.policy_name::name, c.setting, c.expected, c.actual, c.same
        FROM compared c
        WHERE NOT c.same OR NOT check_storage_policy_drift.only_drift
        UNION ALL
        SELECT m.s, m.t, m.policy_name::name, 'tsdb.hypertable', 'true', 'false', false
        FROM plain_matched m
    ) r
    ORDER BY r.s, r.t, r.setting
$$;

-- Bring an existing table up to the storage policy that applies to it, which is what
-- check_storage_policy_drift() reports as different. It returns one row per setting with what
-- it was, what it is set to and what was done. dry_run => true only reports.
--
-- A policy that is changed while this runs may give anomalous results, as the drift it compares
-- against is read again and no lock keeps the policy still.
--
-- This is not SECURITY DEFINER, so it needs the same privileges as making the changes by hand:
-- the caller has to own the table. Whatever fails undoes everything, as it is one transaction.
--
-- * A hypertable gets the chunk interval, the columnstore settings and the compression and
--   retention policies the storage policy declares. The partition column cannot be changed and is
--   an error. A new chunk interval only applies to chunks created from now on.
-- * A plain table that is empty is converted to a hypertable. One with rows is an error, as
--   converting it has to move the data: use create_hypertable(..., migrate_data => true) and
--   apply the policy again.
-- * Retention deletes data, and on an existing table that can be data that is already there. It is
--   only applied with allow_retention => true, otherwise its row says that it was skipped.
--
-- TODO: a new chunk interval leaves the chunks that exist at their old size. merge_chunks() and
-- split_chunk() could bring them in line, but they rewrite data and take locks, so that needs its
-- own design and is not done here.
CREATE OR REPLACE FUNCTION @extschema@.apply_storage_policy(
    schema_name NAME, table_name NAME, dry_run BOOL DEFAULT false, allow_retention BOOL DEFAULT false)
RETURNS TABLE (setting TEXT, expected TEXT, actual TEXT, action TEXT)
LANGUAGE plpgsql VOLATILE SET search_path TO pg_catalog, pg_temp AS
$$
DECLARE
    rel REGCLASS;
    policy_name NAME;
    is_hypertable BOOL;
    d JSONB;
    done TEXT := CASE WHEN apply_storage_policy.dry_run THEN 'would apply' ELSE 'applied' END;
    new_segmentby TEXT;
    new_orderby TEXT;
    chunk TEXT;
    partcol TEXT;
    partcol_type REGTYPE;
    expected_after TEXT;
    clauses TEXT[];
    default_indexes BOOL;
    new_index TEXT;
    is_integer_type BOOL;
    base_type OID;
    compressed_chunks BIGINT;
    exp JSONB;
BEGIN
    rel := to_regclass(format('%I.%I', apply_storage_policy.schema_name, apply_storage_policy.table_name));
    IF rel IS NULL THEN
        RAISE EXCEPTION 'relation "%"."%" does not exist', apply_storage_policy.schema_name,
            apply_storage_policy.table_name USING ERRCODE = 'undefined_table';
    END IF;

    -- a dry run is as strict as the real thing, so it does not promise what would fail. The real
    -- run fails on its first change, with the error of that change.
    IF apply_storage_policy.dry_run
       AND NOT pg_has_role(current_user, (SELECT c.relowner FROM pg_class c WHERE c.oid = rel), 'USAGE') THEN
        RAISE EXCEPTION 'must be owner of table %', apply_storage_policy.table_name
            USING ERRCODE = 'insufficient_privilege';
    END IF;

    -- what the policy says is read once, so that a policy changed meanwhile cannot give one run
    -- pieces of the old and of the new. The setting is for the tables a session creates, so it
    -- does not hide the policy here.
    SELECT jsonb_agg(jsonb_build_object('setting', e.setting, 'value', e.value, 'source', e.source))
    INTO exp
    FROM _timescaledb_functions.storage_policy_explain(apply_storage_policy.schema_name,
                                                       apply_storage_policy.table_name, NULL, true) e;
    policy_name := _timescaledb_functions.storage_policy_setting(exp, 'storage_policy');
    IF policy_name IS NULL THEN
        RAISE EXCEPTION 'no storage policy applies to "%"."%"', apply_storage_policy.schema_name,
            apply_storage_policy.table_name
            USING ERRCODE = 'undefined_object',
                  HINT = 'Use show_storage_policy_matches() to see which policies match a name.';
    END IF;

    is_hypertable := EXISTS (SELECT FROM timescaledb_information.hypertables h
                             WHERE h.hypertable_schema = apply_storage_policy.schema_name
                               AND h.hypertable_name = apply_storage_policy.table_name);

    IF NOT is_hypertable THEN
        DECLARE
            has_rows BOOL;
        BEGIN
            EXECUTE format('SELECT EXISTS (SELECT FROM %s)', rel) INTO has_rows;
            IF has_rows THEN
                RAISE EXCEPTION 'cannot apply storage policy "%" to "%"."%": it is not a hypertable and has rows',
                    policy_name, apply_storage_policy.schema_name, apply_storage_policy.table_name
                    USING ERRCODE = 'feature_not_supported',
                          HINT = 'Convert it with create_hypertable(..., migrate_data => true), then apply the policy again.';
            END IF;
        END;

        partcol := _timescaledb_functions.storage_policy_setting(exp, 'tsdb.partition_column');
        chunk := _timescaledb_functions.storage_policy_setting(exp, 'tsdb.chunk_interval');
        IF partcol IS NULL THEN
            RAISE EXCEPTION 'cannot apply storage policy "%" to "%"."%": the policy has no partition_column',
                policy_name, apply_storage_policy.schema_name, apply_storage_policy.table_name
                USING ERRCODE = 'invalid_parameter_value',
                      HINT = 'A plain table can only be converted when the policy says which column to partition by.';
        END IF;
        SELECT a.atttypid::regtype INTO partcol_type FROM pg_attribute a
        WHERE a.attrelid = rel AND a.attname = partcol AND NOT a.attisdropped;
        IF partcol_type IS NULL THEN
            RAISE EXCEPTION 'cannot apply storage policy "%" to "%"."%": column "%" does not exist',
                policy_name, apply_storage_policy.schema_name, apply_storage_policy.table_name, partcol
                USING ERRCODE = 'undefined_column';
        END IF;

        SELECT CASE WHEN t.typtype = 'd' THEN t.typbasetype ELSE t.oid END INTO base_type
        FROM pg_type t WHERE t.oid = partcol_type::oid;

        -- what create_hypertable would refuse, so that a dry run does not say 'would convert' for a
        -- table that the real run then fails on
        IF apply_storage_policy.dry_run THEN
            IF NOT EXISTS (
                   SELECT FROM pg_type t
                   WHERE t.oid = base_type
                     AND t.oid = ANY (ARRAY['timestamp', 'timestamptz', 'date', 'smallint', 'integer',
                                            'bigint', 'uuid']::regtype[]::oid[])) THEN
                RAISE EXCEPTION 'cannot apply storage policy "%" to "%"."%": invalid type for dimension "%"',
                    policy_name, apply_storage_policy.schema_name, apply_storage_policy.table_name, partcol
                    USING ERRCODE = 'invalid_parameter_value',
                          HINT = 'The partition column must be a timestamp, date, uuid or integer type.';
            END IF;
            IF EXISTS (
                   SELECT FROM pg_index i
                   WHERE i.indrelid = rel AND (i.indisunique OR i.indisprimary)
                     AND NOT EXISTS (SELECT FROM pg_attribute a
                                     WHERE a.attrelid = rel AND a.attname = partcol
                                       AND a.attnum = ANY (i.indkey::int2[]))) THEN
                RAISE EXCEPTION 'cannot apply storage policy "%" to "%"."%": cannot create a unique index without the column "%" (used in partitioning)',
                    policy_name, apply_storage_policy.schema_name, apply_storage_policy.table_name, partcol
                    USING ERRCODE = 'feature_not_supported',
                          HINT = 'Add the column to the primary key and the unique indexes, or choose another partition column.';
            END IF;
        END IF;

        setting := 'tsdb.hypertable'; expected := 'true'; actual := 'false';
        action := CASE WHEN apply_storage_policy.dry_run THEN 'would convert' ELSE 'converted' END;
        RETURN NEXT;

        is_integer_type := base_type = ANY (ARRAY['smallint', 'integer', 'bigint']::regtype[]::oid[]);
        default_indexes := COALESCE(_timescaledb_functions.storage_policy_setting(exp, 'tsdb.create_default_indexes'), 't') = 't';

        IF apply_storage_policy.dry_run THEN
            -- what a converted table would be set to, as CREATE TABLE sets it: the columnstore is
            -- on, and so is a compression policy at the chunk interval unless the policy says
            -- something else
            IF current_setting('timescaledb.license') <> 'apache' THEN
                setting := 'tsdb.columnstore'; expected := 'true'; actual := NULL; action := 'would apply';
                RETURN NEXT;
                IF NOT is_integer_type AND _timescaledb_functions.storage_policy_setting(exp, 'tsdb.compress_after') IS NULL THEN
                    setting := 'tsdb.compress_after'; expected := COALESCE(chunk, '7 days');
                    actual := NULL; action := 'would apply';
                    RETURN NEXT;
                END IF;
            END IF;
            RETURN QUERY
            SELECT e.setting, e.value, NULL::text,
                   CASE WHEN e.setting = 'retention_after' AND e.value <> 'never'
                             AND NOT apply_storage_policy.allow_retention
                        THEN 'skipped: retention can delete existing data, pass allow_retention => true'
                        ELSE 'would apply' END
            FROM jsonb_to_recordset(exp) AS e(setting TEXT, value TEXT, source TEXT)
            -- the partition column and the chunk interval are part of the conversion, and a
            -- converted table has no retention policy, so NEVER is already how it is
            WHERE e.source = 'policy'
              AND e.setting NOT IN ('tsdb.hypertable', 'tsdb.partition_column', 'tsdb.chunk_interval',
                                    'tsdb.create_default_indexes', 'tsdb.columnstore')
              AND NOT (e.setting = 'retention_after' AND e.value = 'never');
            RETURN;
        END IF;

        IF chunk IS NULL THEN
            PERFORM @extschema@.create_hypertable(rel, @extschema@.by_range(partcol::name),
                                                  create_default_indexes => default_indexes);
        ELSIF NOT is_integer_type THEN
            PERFORM @extschema@.create_hypertable(rel, @extschema@.by_range(partcol::name, chunk::interval),
                                                  create_default_indexes => default_indexes);
        ELSE
            PERFORM @extschema@.create_hypertable(rel, @extschema@.by_range(partcol::name, chunk::bigint),
                                                  create_default_indexes => default_indexes);
        END IF;

        -- the columnstore, as CREATE TABLE enables it. The ages are left to what follows, which
        -- applies what the policy declares.
        IF current_setting('timescaledb.license') <> 'apache' THEN
            clauses := ARRAY['timescaledb.compress'];
            new_segmentby := _timescaledb_functions.storage_policy_setting(exp, 'tsdb.segmentby');
            new_orderby := _timescaledb_functions.storage_policy_setting(exp, 'tsdb.orderby');
            new_index := _timescaledb_functions.storage_policy_setting(exp, 'tsdb.compress_index');
            IF new_segmentby IS NOT NULL THEN
                clauses := clauses || format('timescaledb.compress_segmentby = %L', new_segmentby);
            END IF;
            IF new_orderby IS NOT NULL THEN
                clauses := clauses || format('timescaledb.compress_orderby = %L', new_orderby);
            END IF;
            IF new_index IS NOT NULL THEN
                clauses := clauses || format('timescaledb.compress_index = %L', new_index);
            END IF;
            EXECUTE format('ALTER TABLE %s SET (%s)', rel, array_to_string(clauses, ', '));
            setting := 'tsdb.columnstore'; expected := 'true'; actual := NULL; action := 'applied';
            RETURN NEXT;
            IF new_segmentby IS NOT NULL THEN
                setting := 'tsdb.segmentby'; expected := new_segmentby; RETURN NEXT;
            END IF;
            IF new_orderby IS NOT NULL THEN
                setting := 'tsdb.orderby'; expected := new_orderby; RETURN NEXT;
            END IF;
            IF new_index IS NOT NULL THEN
                setting := 'tsdb.compress_index'; expected := new_index; RETURN NEXT;
            END IF;

            IF NOT is_integer_type AND _timescaledb_functions.storage_policy_setting(exp, 'tsdb.compress_after') IS NULL THEN
                PERFORM @extschema@.add_compression_policy(rel, (
                    SELECT dm.time_interval FROM timescaledb_information.dimensions dm
                    WHERE dm.hypertable_schema = apply_storage_policy.schema_name
                      AND dm.hypertable_name = apply_storage_policy.table_name
                      AND dm.dimension_number = 1));
                setting := 'tsdb.compress_after'; expected := COALESCE(chunk, '7 days');
                RETURN NEXT;
            END IF;
        END IF;
    END IF;

    SELECT jsonb_object_agg(x.setting, jsonb_build_object('expected', x.expected, 'actual', x.actual))
    INTO d
    FROM @extschema@.check_storage_policy_drift(apply_storage_policy.schema_name,
                                                apply_storage_policy.table_name) x;
    d := COALESCE(d, '{}');

    IF d ? 'tsdb.partition_column' THEN
        RAISE EXCEPTION 'cannot apply storage policy "%" to "%"."%": its partition column is "%", the policy says "%"',
            policy_name, apply_storage_policy.schema_name, apply_storage_policy.table_name,
            d->'tsdb.partition_column'->>'actual', d->'tsdb.partition_column'->>'expected'
            USING ERRCODE = 'feature_not_supported',
                  HINT = 'The partition column of a hypertable cannot be changed.';
    END IF;

    IF d ? 'tsdb.chunk_interval' THEN
        IF NOT apply_storage_policy.dry_run THEN
            chunk := d->'tsdb.chunk_interval'->>'expected';
            IF EXISTS (SELECT FROM timescaledb_information.dimensions dm
                       WHERE dm.hypertable_schema = apply_storage_policy.schema_name
                         AND dm.hypertable_name = apply_storage_policy.table_name
                         AND dm.dimension_number = 1 AND dm.time_interval IS NOT NULL) THEN
                PERFORM @extschema@.set_chunk_time_interval(rel, chunk::interval);
            ELSE
                PERFORM @extschema@.set_chunk_time_interval(rel, chunk::bigint);
            END IF;
        END IF;
        setting := 'tsdb.chunk_interval'; expected := d->'tsdb.chunk_interval'->>'expected';
        actual := d->'tsdb.chunk_interval'->>'actual'; action := done;
        RETURN NEXT;
    END IF;

    -- the columnstore settings are set together, as setting one alone can reset the other
    IF d ? 'tsdb.segmentby' OR d ? 'tsdb.orderby' THEN
        clauses := ARRAY['timescaledb.compress'];
        new_segmentby := _timescaledb_functions.storage_policy_setting(exp, 'tsdb.segmentby');
        new_orderby := _timescaledb_functions.storage_policy_setting(exp, 'tsdb.orderby');
        IF new_segmentby IS NOT NULL THEN
            clauses := clauses || format('timescaledb.compress_segmentby = %L', new_segmentby);
        END IF;
        IF new_orderby IS NOT NULL THEN
            clauses := clauses || format('timescaledb.compress_orderby = %L', new_orderby);
        END IF;
        IF NOT apply_storage_policy.dry_run THEN
            EXECUTE format('ALTER TABLE %s SET (%s)', rel, array_to_string(clauses, ', '));
        END IF;
        -- chunks that are compressed already are not compressed again with the new settings
        SELECT count(*) INTO compressed_chunks FROM timescaledb_information.chunks c
        WHERE c.hypertable_schema = apply_storage_policy.schema_name
          AND c.hypertable_name = apply_storage_policy.table_name AND c.is_compressed;
        IF d ? 'tsdb.segmentby' THEN
            setting := 'tsdb.segmentby'; expected := d->'tsdb.segmentby'->>'expected';
            actual := d->'tsdb.segmentby'->>'actual'; action := done;
            IF compressed_chunks > 0 THEN
                action := action || ', the compressed chunks that exist keep their old settings';
            END IF;
            RETURN NEXT;
        END IF;
        IF d ? 'tsdb.orderby' THEN
            setting := 'tsdb.orderby'; expected := d->'tsdb.orderby'->>'expected';
            actual := d->'tsdb.orderby'->>'actual'; action := done;
            IF compressed_chunks > 0 THEN
                action := action || ', the compressed chunks that exist keep their old settings';
            END IF;
            RETURN NEXT;
        END IF;
    END IF;

    IF d ? 'tsdb.compress_after' THEN
        expected_after := d->'tsdb.compress_after'->>'expected';
        IF NOT apply_storage_policy.dry_run THEN
            PERFORM @extschema@.remove_compression_policy(rel, if_exists => true);
            IF expected_after <> 'never' THEN
                -- the policy needs columnstore, which a table with no settings to change lacks
                IF NOT EXISTS (SELECT FROM timescaledb_information.hypertables h
                               WHERE h.hypertable_schema = apply_storage_policy.schema_name
                                 AND h.hypertable_name = apply_storage_policy.table_name
                                 AND h.compression_enabled) THEN
                    EXECUTE format('ALTER TABLE %s SET (timescaledb.compress)', rel);
                END IF;
                PERFORM @extschema@.add_compression_policy(rel, expected_after::interval);
            END IF;
        END IF;
        setting := 'tsdb.compress_after'; expected := expected_after;
        actual := d->'tsdb.compress_after'->>'actual'; action := done;
        RETURN NEXT;
    END IF;

    IF d ? 'retention_after' THEN
        setting := 'retention_after'; expected := d->'retention_after'->>'expected';
        actual := d->'retention_after'->>'actual';
        IF expected = 'never' THEN
            -- removing the policy deletes nothing, so it needs no allow_retention
            IF NOT apply_storage_policy.dry_run THEN
                PERFORM @extschema@.remove_retention_policy(rel, if_exists => true);
            END IF;
            action := done;
        ELSIF apply_storage_policy.allow_retention THEN
            IF NOT apply_storage_policy.dry_run THEN
                PERFORM @extschema@.remove_retention_policy(rel, if_exists => true);
                PERFORM @extschema@.add_retention_policy(rel, expected::interval);
            END IF;
            action := done;
        ELSE
            action := 'skipped: retention can delete existing data, pass allow_retention => true';
        END IF;
        RETURN NEXT;
    END IF;
END
$$;
