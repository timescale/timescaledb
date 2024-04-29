-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\set ECHO queries

\ir include/hyperstore_helpers.sql

select setseed(1);

\set the_table test_float
\set the_type float
\set the_generator ceil(random()*10)
\set the_aggregate sum(value)
\set the_clause value > 0.5
\ir include/hyperstore_type_table.sql

\set the_table test_numeric
\set the_type numeric(5,2)
\set the_generator ceil(random()*10)
\set the_aggregate sum(value)
\set the_clause value > 0.5
\ir include/hyperstore_type_table.sql

\set the_table test_bool
\set the_type boolean
\set the_generator (random() > 0.5)
\set the_aggregate count(value)
\set the_clause value = true
\ir include/hyperstore_type_table.sql

