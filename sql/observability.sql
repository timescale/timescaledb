-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

-- Set-returning functions that expose the in-memory observability segment
-- (see src/observ/ts_observ_srf.c) to SQL.

CREATE OR REPLACE FUNCTION _timescaledb_functions.observ_keys(
    since TIMESTAMPTZ DEFAULT NULL
) RETURNS TABLE(
    key_id       SMALLINT,
    key_name     TEXT,
    description  TEXT,
    total_count  BIGINT,
    first_seen   TIMESTAMPTZ,
    last_seen    TIMESTAMPTZ
) AS '@MODULE_PATHNAME@', 'ts_observ_keys'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.observ_log(
    filter JSONB        DEFAULT NULL,
    since  TIMESTAMPTZ  DEFAULT NULL,
    keys   TEXT[]       DEFAULT NULL
) RETURNS TABLE(
    event_id    BIGINT,
    event_time  TIMESTAMPTZ,
    key_name    TEXT,
    value       DOUBLE PRECISION
) AS '@MODULE_PATHNAME@', 'ts_observ_log'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.observ_key_values(
    key    TEXT,
    filter JSONB       DEFAULT NULL,
    since  TIMESTAMPTZ DEFAULT NULL
) RETURNS TABLE(
    event_time TIMESTAMPTZ,
    value      DOUBLE PRECISION
) AS '@MODULE_PATHNAME@', 'ts_observ_key_values'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.observ_log_events(
    filter JSONB       DEFAULT NULL,
    since  TIMESTAMPTZ DEFAULT NULL
) RETURNS TABLE(
    event_id    BIGINT,
    event_time  TIMESTAMPTZ,
    event_type  TEXT,
    payload     JSONB
) AS '@MODULE_PATHNAME@', 'ts_observ_log_events'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION _timescaledb_functions.observ_reset()
RETURNS VOID
AS '@MODULE_PATHNAME@', 'ts_observ_reset'
LANGUAGE C VOLATILE;
