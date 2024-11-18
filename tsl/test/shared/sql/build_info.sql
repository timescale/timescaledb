-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

-- Check what get_git_commit returns
SELECT pg_typeof(commit_tag) AS commit_tag_type,
       pg_typeof(commit_hash) AS commit_hash_type,
       length(commit_hash) AS commit_hash_length,
       pg_typeof(commit_time) AS commit_time_type
  FROM _timescaledb_functions.get_git_commit();

SELECT extname, obj_description(oid, 'pg_extension') FROM pg_extension WHERE extname = 'timescaledb';

