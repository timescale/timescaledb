-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\ir post.v11.sql

-- int2 bloom fixture is skipped for singlestep mode
SELECT :'UPDATE_MODE' <> 'singlestep' AS run_int2_bloom \gset
\if :run_int2_bloom
\ir post.int2_bloom_migration.sql
\endif
