# This file and its contents are licensed under the Timescale License.
# Please see the included NOTICE for copyright information and
# LICENSE-TIMESCALE for a copy of the license.

###
# A storage policy that is changed while tables are created. CREATE TABLE reads the policy
# with a snapshot of the committed state, so it does not wait for the change and sees the old
# policy until the change commits, and the new one after. It never ends up with neither, which
# would leave the table plain without a word.
###

setup
{
    DO $$
    BEGIN
        PERFORM add_storage_policy('sp_chg', 'public', 'sp_c_.*',
            jsonb_build_object('partition_column', 'ts', 'chunk_interval', '1 day'),
            match_kind => 'regex', priority => 1);
    END
    $$;
}

teardown
{
    DROP TABLE IF EXISTS sp_c_1, sp_c_2;
    DO $$
    BEGIN
        PERFORM remove_storage_policy('sp_chg');
    END
    $$;
}

session "A"
step "a_begin"  { BEGIN; }
step "a_alter"  { SELECT alter_storage_policy('sp_chg', policy => jsonb_build_object('partition_column', 'ts', 'chunk_interval', '3 days')); }
step "a_commit"  { COMMIT; }

session "B"
step "b_create_1"  { CREATE TABLE sp_c_1 (ts timestamptz NOT NULL, v int); }
step "b_create_2"  { CREATE TABLE sp_c_2 (ts timestamptz NOT NULL, v int); }

session "V"
step "check"
{
    SELECT hypertable_name, time_interval FROM timescaledb_information.dimensions
    WHERE hypertable_name IN ('sp_c_1', 'sp_c_2') ORDER BY 1;
}

permutation "a_begin" "a_alter" "b_create_1" "a_commit" "b_create_2" "check"
