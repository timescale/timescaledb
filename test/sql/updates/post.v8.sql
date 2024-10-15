-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\set PG_UPGRADE_TEST false
\ir post.catalog.sql
\unset PG_UPGRADE_TEST
\ir post.insert.sql
\ir post.integrity_test.sql
\ir catalog_missing_columns.sql
\ir post.compression.sql
\ir post.continuous_aggs.v3.sql
\ir post.policies.sql
\ir post.sequences.sql
\ir post.functions.sql
