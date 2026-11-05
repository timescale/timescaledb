DROP FUNCTION IF EXISTS _timescaledb_functions.bloom1_contains(_timescaledb_internal.bloom1, anyelement);

-- add storage_policy
CREATE TABLE _timescaledb_catalog.storage_policy (
  id serial NOT NULL,
  name name NOT NULL,
  schema_name text NOT NULL,
  table_name text NOT NULL,
  match_kind text NOT NULL,
  priority integer,
  policy jsonb NOT NULL,
  enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  updated_by name NOT NULL DEFAULT current_user,
  -- table constraints
  CONSTRAINT storage_policy_pkey PRIMARY KEY (id),
  CONSTRAINT storage_policy_name_key UNIQUE (name),
  CONSTRAINT storage_policy_match_kind_check CHECK (match_kind IN ('exact', 'regex')),
  CONSTRAINT storage_policy_priority_check CHECK ((match_kind = 'exact') = (priority IS NULL))
);

CREATE UNIQUE INDEX storage_policy_exact_key ON _timescaledb_catalog.storage_policy (schema_name, table_name) WHERE match_kind = 'exact';
CREATE UNIQUE INDEX storage_policy_priority_key ON _timescaledb_catalog.storage_policy (priority) WHERE match_kind = 'regex';

SELECT pg_catalog.pg_extension_config_dump('_timescaledb_catalog.storage_policy', '');
SELECT pg_catalog.pg_extension_config_dump(pg_get_serial_sequence('_timescaledb_catalog.storage_policy', 'id'), '');

