-- This file and its contents are licensed under the Timescale License.
-- Please see the included NOTICE for copyright information and
-- LICENSE-TIMESCALE for a copy of the license.

\ir include/hypercore_helpers.sql

select setseed(1);

-- Test that decompressing and scannning floats work. These are batch
-- decompressable and can be vectorized.
\set the_table test_float
\set the_type float
\set the_generator ceil(random()*10)
\set the_aggregate sum(value)
\set the_clause value > 0.5
\ir include/hypercore_type_table.sql

-- Test that decompressing and scanning numerics works. These are not
-- batch decompressable.
\set the_table test_numeric
\set the_type numeric(5,2)
\set the_generator ceil(random()*10)
\set the_aggregate sum(value)
\set the_clause value > 0.5
\ir include/hypercore_type_table.sql

-- Test that decompressing and scanning boolean columns works.
\set the_table test_bool
\set the_type boolean
\set the_generator (random() > 0.5)
\set the_aggregate count(value)
\set the_clause value = true
\ir include/hypercore_type_table.sql

\set my_uuid e0317dfc-77cd-46da-a4e9-8626ce49ccad

-- Test that text works with a simple comparison with a constant
-- value.
\set the_table test_text
\set the_type text
\set the_generator gen_random_uuid()::text
\set the_aggregate count(*)
\set the_clause value = :'my_uuid'
\ir include/hypercore_type_table.sql

-- Test that we can decompress and scan JSON fields without
-- filters. This just tests that decompression works.
\set a_name temp
\set the_table test_jsonb
\set the_type jsonb
\set the_generator jsonb_build_object(:'a_name',round(random()*100))
\set the_aggregate sum((value->:'a_name')::int)
\set the_clause true
\ir include/hypercore_type_table.sql

-- Test that we can decompress and scan JSON fields with a filter
-- using JSON operators (these are function calls, so they do not have
-- simple scan keys).
\set a_name temp
\set the_table test_jsonb
\set the_type jsonb
\set the_generator jsonb_build_object(:'a_name',round(random()*100))
\set the_aggregate sum((value->:'a_name')::int)
\set the_clause ((value->:'a_name')::numeric >= 0.5) and ((value->:'a_name')::numeric <= 0.6)
\ir include/hypercore_type_table.sql

-- Test that we can use NAME type for a field and compare with a
-- constant value. This is a fixed-size type with a size > 8 and we
-- will try to use a scan key for this.
\set a_name temp
\set the_table test_name
\set the_type name
\set the_generator gen_random_uuid()::name
\set the_aggregate count(*)
\set the_clause value = :'my_uuid'
\ir include/hypercore_type_table.sql
