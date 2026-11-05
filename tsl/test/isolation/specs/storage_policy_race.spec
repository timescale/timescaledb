# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

# Two writers start up at the same time and each makes sure the table exists before it inserts,
# as parallel Flink or Kafka Connect sinks do for a new site. Neither of them knows about
# hypertables: they only send CREATE TABLE IF NOT EXISTS and INSERT. A storage policy turns the
# table into a hypertable inside the CREATE TABLE, so no interleaving can leave rows in a plain
# table or leave the table without its policies.

# The spec language ends a block at the first closing brace, so the policy is built without
# writing a JSON object literal.
setup
{
    DO $$
    BEGIN
        PERFORM add_storage_policy('sp_race', 'public', 'sp_t',
            jsonb_build_object(
                'partition_column', 'ts',
                'chunk_interval', '1 day',
                'columnstore', jsonb_build_object('segmentby', 'device'),
                'compression', jsonb_build_object('mode', 'AFTER', 'after', '7 days'),
                'retention', jsonb_build_object('mode', 'AFTER', 'after', '2 years')));
    END
    $$;
}

teardown
{
    DROP TABLE IF EXISTS sp_t, sp_plain;
    DO $$
    BEGIN
        PERFORM remove_storage_policy('sp_race');
    END
    $$;
}

session "A"
step "a_create"  { CREATE TABLE IF NOT EXISTS sp_t (ts timestamptz NOT NULL, device int, value float8); }
step "a_begin_create"  { BEGIN; CREATE TABLE IF NOT EXISTS sp_t (ts timestamptz NOT NULL, device int, value float8); }
step "a_insert"  { INSERT INTO sp_t VALUES ('2025-01-01', 1, 1.0); }
step "a_commit"  { COMMIT; }
# a client that sets the table up itself as well, as an add-on or a startup script would
step "a_ht_if_not_exists"  { SELECT create_hypertable('sp_t', 'ts', if_not_exists => true) IS NOT NULL AS created; }
step "a_ht_plain"  { SELECT create_hypertable('sp_t', 'ts') IS NOT NULL AS created; }
step "a_policy_if_not_exists"  { SELECT add_compression_policy('sp_t', INTERVAL '7 days', if_not_exists => true) IS NOT NULL AS added; }
step "a_policy_plain"  { SELECT add_compression_policy('sp_t', INTERVAL '7 days') IS NOT NULL AS added; }
# the way a client has to do it without a storage policy, on a table no policy matches
step "a_plain_create"  { CREATE TABLE IF NOT EXISTS sp_plain (ts timestamptz NOT NULL, device int, value float8); }
step "a_make_hypertable"  { SELECT create_hypertable('sp_plain', 'ts', if_not_exists => true) IS NOT NULL AS created; }

session "B"
step "b_create"  { CREATE TABLE IF NOT EXISTS sp_t (ts timestamptz NOT NULL, device int, value float8); }
step "b_create_retry"  { CREATE TABLE IF NOT EXISTS sp_t (ts timestamptz NOT NULL, device int, value float8); }
step "b_insert"  { INSERT INTO sp_t VALUES ('2025-01-02', 2, 2.0); }
step "b_plain_create"  { CREATE TABLE IF NOT EXISTS sp_plain (ts timestamptz NOT NULL, device int, value float8); }
step "b_plain_insert"  { INSERT INTO sp_plain VALUES ('2025-01-02', 2, 2.0); }

session "V"
# exactly one hypertable, both rows in chunks and none in the parent, one job of each kind
step "check"
{
    SELECT
        (SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'sp_t') AS hypertables,
        (SELECT count(*) FROM sp_t) AS rows,
        (SELECT count(*) FROM ONLY sp_t) AS rows_in_parent,
        (SELECT count(*) FROM show_chunks('sp_t')) AS chunks,
        (SELECT count(*) FROM timescaledb_information.jobs
          WHERE hypertable_name = 'sp_t' AND proc_name = 'policy_compression') AS compression_jobs,
        (SELECT count(*) FROM timescaledb_information.jobs
          WHERE hypertable_name = 'sp_t' AND proc_name = 'policy_retention') AS retention_jobs,
        (SELECT count(*) FROM timescaledb_information.hypertable_columnstore_settings
          WHERE hypertable = 'sp_t'::regclass AND segmentby = 'device') AS columnstore_settings;
}

# every interleaving of the two writers, each doing CREATE TABLE IF NOT EXISTS then INSERT
permutation "a_create" "a_insert" "b_create" "b_insert" "check"
permutation "a_create" "b_create" "a_insert" "b_insert" "check"
permutation "a_create" "b_create" "b_insert" "a_insert" "check"
permutation "b_create" "a_create" "a_insert" "b_insert" "check"
permutation "b_create" "a_create" "b_insert" "a_insert" "check"
permutation "b_create" "b_insert" "a_create" "a_insert" "check"

# both create the table in transactions that overlap. PostgreSQL itself fails the second
# CREATE TABLE IF NOT EXISTS with a unique violation, and a plain retry then finds a complete table.
permutation "a_begin_create" "b_create" "a_commit" "b_create_retry" "b_insert" "a_insert" "check"

# a client that also does its own setup finds the work done: with if_not_exists the calls are no-ops,
# without it they fail, and either way the table ends up as before
permutation "a_create" "b_create" "a_ht_if_not_exists" "a_policy_if_not_exists" "b_insert" "check"
permutation "a_create" "b_create" "a_ht_plain" "a_policy_plain" "b_insert" "check"

# without a policy, the insert from the second writer lands in a plain table, and the first writer
# can no longer make it a hypertable
permutation "a_plain_create" "b_plain_create" "b_plain_insert" "a_make_hypertable"
