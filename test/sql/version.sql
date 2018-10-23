-- Test that get_git_commit returns text
select pg_typeof(git) from _timescaledb_internal.get_git_commit() AS git;

-- Test that get_os_info returns 3 x text
select pg_typeof(sysname) AS sysname_type,pg_typeof(version) AS version_type,pg_typeof(release) AS release_type from _timescaledb_internal.get_os_info();

-- Test that get_version major, minor and patch are int
select pg_typeof(major) AS major_type,pg_typeof(minor) AS minor_type,pg_typeof(patch) AS patch_type from _timescaledb_internal.get_version();

