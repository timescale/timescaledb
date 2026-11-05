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

-- A column list as the hypertable reports it and as a policy spells it differ in ways that are not
-- a difference: the case, the spaces around the commas, and an ASC the table does not keep
-- because it is the default.
CREATE OR REPLACE FUNCTION _timescaledb_functions.storage_policy_normalize_columns(cols TEXT)
RETURNS TEXT
LANGUAGE sql IMMUTABLE PARALLEL SAFE SET search_path TO pg_catalog, pg_temp AS
$$
    SELECT lower(btrim(regexp_replace(regexp_replace(regexp_replace(
        cols, '\s+asc\y', '', 'gi'), '\s*,\s*', ',', 'g'), '\s+', ' ', 'g')))
$$;

-- Where existing hypertables no longer match the storage policy that applies to them today.
-- Policies are applied once, when a table is created, so a table keeps the settings it was
-- created with when the policy is edited, when someone changes the table by hand, or when it was
-- created with its own options. This compares what explain_storage_policy() says a new table with
-- the same name would get against the hypertable, and returns one row per setting that differs.
-- Only what the policy declares is compared, so a setting the policy leaves out is never drift,
-- and a hypertable no policy matches is not listed. The settings compared are the partition
-- column, the chunk interval, the columnstore segmentby and orderby, and the ages of the
-- compression and retention policies.
--
-- With no arguments every hypertable is analysed, with a schema only the hypertables in it, and
-- with a schema and a table just that one. Pass only_drift => false to get every setting that was
-- compared, with in_sync saying whether it matches, to see how a table stands against its policy.
CREATE OR REPLACE FUNCTION @extschema@.check_storage_policy_drift(
    schema_name NAME DEFAULT NULL, table_name NAME DEFAULT NULL, only_drift BOOL DEFAULT true)
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
    ), want AS (
        SELECT hts.s, hts.t, e.setting, e.value AS expected, e.source,
               max(e.value) FILTER (WHERE e.setting = 'storage_policy')
                   OVER (PARTITION BY hts.s, hts.t) AS policy_name
        FROM hts
        CROSS JOIN LATERAL @extschema@.explain_storage_policy(hts.s, hts.t) e
    ), compared AS (
        SELECT w.*, a.actual, a.same
        FROM want w
        CROSS JOIN LATERAL (
            SELECT
                CASE w.setting
                    WHEN 'tsdb.partition_column' THEN d.column_name::text
                    WHEN 'tsdb.chunk_interval' THEN coalesce(d.time_interval::text, d.integer_interval::text)
                    WHEN 'tsdb.segmentby' THEN cs.segmentby
                    WHEN 'tsdb.orderby' THEN cs.orderby
                    WHEN 'tsdb.compress_after' THEN cj.config->>'compress_after'
                    WHEN 'retention_after' THEN rj.config->>'drop_after'
                END AS actual,
                CASE w.setting
                    WHEN 'tsdb.partition_column' THEN d.column_name::text IS NOT DISTINCT FROM w.expected
                    -- an integer time column has an integer chunk interval, not an interval
                    WHEN 'tsdb.chunk_interval' THEN
                        CASE WHEN d.integer_interval IS NOT NULL
                             THEN d.integer_interval::text IS NOT DISTINCT FROM btrim(w.expected)
                             ELSE d.time_interval IS NOT DISTINCT FROM w.expected::interval
                        END
                    -- the lists are compared without the spaces and the case
                    WHEN 'tsdb.segmentby' THEN
                        _timescaledb_functions.storage_policy_normalize_columns(cs.segmentby)
                        IS NOT DISTINCT FROM _timescaledb_functions.storage_policy_normalize_columns(w.expected)
                    WHEN 'tsdb.orderby' THEN
                        _timescaledb_functions.storage_policy_normalize_columns(cs.orderby)
                        IS NOT DISTINCT FROM _timescaledb_functions.storage_policy_normalize_columns(w.expected)
                    -- NEVER means there is no job
                    WHEN 'tsdb.compress_after' THEN
                        CASE WHEN w.expected = 'never' THEN cj.config IS NULL
                             ELSE (cj.config->>'compress_after')::interval IS NOT DISTINCT FROM w.expected::interval
                        END
                    WHEN 'retention_after' THEN
                        (rj.config->>'drop_after')::interval IS NOT DISTINCT FROM w.expected::interval
                END AS same
            FROM (SELECT 1) AS one
            LEFT JOIN timescaledb_information.dimensions d
                   ON d.hypertable_schema = w.s AND d.hypertable_name = w.t AND d.dimension_number = 1
            LEFT JOIN timescaledb_information.hypertable_columnstore_settings cs
                   ON cs.hypertable = to_regclass(format('%I.%I', w.s, w.t))
            LEFT JOIN timescaledb_information.jobs cj
                   ON cj.hypertable_schema = w.s AND cj.hypertable_name = w.t
                  AND cj.proc_name = 'policy_compression'
            LEFT JOIN timescaledb_information.jobs rj
                   ON rj.hypertable_schema = w.s AND rj.hypertable_name = w.t
                  AND rj.proc_name = 'policy_retention'
        ) a
        WHERE w.source = 'policy' AND w.policy_name IS NOT NULL
          AND w.setting IN ('tsdb.partition_column', 'tsdb.chunk_interval', 'tsdb.segmentby',
                            'tsdb.orderby', 'tsdb.compress_after', 'retention_after')
    )
    SELECT c.s, c.t, c.policy_name::name, c.setting, c.expected, c.actual, c.same
    FROM compared c
    WHERE NOT c.same OR NOT check_storage_policy_drift.only_drift
    ORDER BY c.s, c.t, c.setting
$$;

-- Bring an existing table up to the storage policy that applies to it, which is what
-- check_storage_policy_drift() reports as different. It returns one row per setting with what
-- it was, what it is set to and what was done. dry_run => true only reports.
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
BEGIN
    rel := to_regclass(format('%I.%I', apply_storage_policy.schema_name, apply_storage_policy.table_name));
    IF rel IS NULL THEN
        RAISE EXCEPTION 'relation "%"."%" does not exist', apply_storage_policy.schema_name,
            apply_storage_policy.table_name USING ERRCODE = 'undefined_table';
    END IF;

    SELECT e.value INTO policy_name
    FROM @extschema@.explain_storage_policy(apply_storage_policy.schema_name,
                                            apply_storage_policy.table_name) e
    WHERE e.setting = 'storage_policy';
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

        SELECT e.value INTO partcol FROM @extschema@.explain_storage_policy(
            apply_storage_policy.schema_name, apply_storage_policy.table_name) e
        WHERE e.setting = 'tsdb.partition_column';
        SELECT e.value INTO chunk FROM @extschema@.explain_storage_policy(
            apply_storage_policy.schema_name, apply_storage_policy.table_name) e
        WHERE e.setting = 'tsdb.chunk_interval';
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

        setting := 'tsdb.hypertable'; expected := 'true'; actual := 'false';
        action := CASE WHEN apply_storage_policy.dry_run THEN 'would convert' ELSE 'converted' END;
        RETURN NEXT;

        IF apply_storage_policy.dry_run THEN
            -- the rest is what a converted table would be set to
            RETURN QUERY
            SELECT e.setting, e.value, NULL::text, 'would apply'
            FROM @extschema@.explain_storage_policy(apply_storage_policy.schema_name,
                                                    apply_storage_policy.table_name) e
            -- the partition column and the chunk interval are part of the conversion
            WHERE e.source = 'policy'
              AND e.setting NOT IN ('tsdb.hypertable', 'tsdb.partition_column', 'tsdb.chunk_interval')
              AND (e.setting <> 'retention_after' OR apply_storage_policy.allow_retention);
            RETURN;
        END IF;

        IF chunk IS NULL THEN
            PERFORM @extschema@.create_hypertable(rel, @extschema@.by_range(partcol::name));
        ELSIF partcol_type = ANY (ARRAY['timestamp', 'timestamptz', 'date']::regtype[]) THEN
            PERFORM @extschema@.create_hypertable(rel, @extschema@.by_range(partcol::name, chunk::interval));
        ELSE
            PERFORM @extschema@.create_hypertable(rel, @extschema@.by_range(partcol::name, chunk::bigint));
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
        SELECT e.value INTO new_segmentby FROM @extschema@.explain_storage_policy(
            apply_storage_policy.schema_name, apply_storage_policy.table_name) e
        WHERE e.setting = 'tsdb.segmentby';
        SELECT e.value INTO new_orderby FROM @extschema@.explain_storage_policy(
            apply_storage_policy.schema_name, apply_storage_policy.table_name) e
        WHERE e.setting = 'tsdb.orderby';
        IF new_segmentby IS NOT NULL THEN
            clauses := clauses || format('timescaledb.compress_segmentby = %L', new_segmentby);
        END IF;
        IF new_orderby IS NOT NULL THEN
            clauses := clauses || format('timescaledb.compress_orderby = %L', new_orderby);
        END IF;
        IF NOT apply_storage_policy.dry_run THEN
            EXECUTE format('ALTER TABLE %s SET (%s)', rel, array_to_string(clauses, ', '));
        END IF;
        IF d ? 'tsdb.segmentby' THEN
            setting := 'tsdb.segmentby'; expected := d->'tsdb.segmentby'->>'expected';
            actual := d->'tsdb.segmentby'->>'actual'; action := done;
            RETURN NEXT;
        END IF;
        IF d ? 'tsdb.orderby' THEN
            setting := 'tsdb.orderby'; expected := d->'tsdb.orderby'->>'expected';
            actual := d->'tsdb.orderby'->>'actual'; action := done;
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
        IF apply_storage_policy.allow_retention THEN
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
