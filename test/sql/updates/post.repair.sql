-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

--Check if the repaired cagg with joins work alright now
\ir post.repair.cagg_joins.sql
\ir post.repair.hierarchical_cagg.sql

\z repair_test_int
\z repair_test_extra


