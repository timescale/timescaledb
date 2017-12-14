\unset ECHO
\o /dev/null
\ir include/test_utils.sql
\o
\set ECHO errors
\set VERBOSITY default

\o /dev/null

--open
SELECT assert_equal(0::bigint, actual_range_start), assert_equal(10::bigint, actual_range_end)
FROM _timescaledb_internal.dimension_calculate_default_range_open(0,10) AS res(actual_range_start, actual_range_end);

SELECT assert_equal(0::bigint, actual_range_start), assert_equal(10::bigint, actual_range_end)
FROM _timescaledb_internal.dimension_calculate_default_range_open(9,10) AS res(actual_range_start, actual_range_end);

SELECT assert_equal(10::bigint, actual_range_start), assert_equal(20::bigint, actual_range_end)
FROM _timescaledb_internal.dimension_calculate_default_range_open(10,10) AS res(actual_range_start, actual_range_end);

SELECT assert_equal(-10::bigint, actual_range_start), assert_equal(0::bigint, actual_range_end)
FROM _timescaledb_internal.dimension_calculate_default_range_open(-1,10) AS res(actual_range_start, actual_range_end);

SELECT assert_equal(-10::bigint, actual_range_start), assert_equal(0::bigint, actual_range_end)
FROM _timescaledb_internal.dimension_calculate_default_range_open(-10,10) AS res(actual_range_start, actual_range_end);

SELECT assert_equal(-20::bigint, actual_range_start), assert_equal(-10::bigint, actual_range_end)
FROM _timescaledb_internal.dimension_calculate_default_range_open(-11,10) AS res(actual_range_start, actual_range_end);

--test that the ends are cut as needed to prevent overflow/undeflow.
SELECT assert_equal(-9223372036854775808, actual_range_start), assert_equal(-9223372036854775800::bigint, actual_range_end)
FROM _timescaledb_internal.dimension_calculate_default_range_open(-9223372036854775808,10) AS res(actual_range_start, actual_range_end);

SELECT assert_equal(-9223372036854775808, actual_range_start), assert_equal(-9223372036854775800::bigint, actual_range_end)
FROM _timescaledb_internal.dimension_calculate_default_range_open(-9223372036854775807,10) AS res(actual_range_start, actual_range_end);

SELECT assert_equal(9223372036854775800::bigint, actual_range_start), assert_equal(9223372036854775807::bigint, actual_range_end)
FROM _timescaledb_internal.dimension_calculate_default_range_open(9223372036854775807,10) AS res(actual_range_start, actual_range_end);

SELECT assert_equal(9223372036854775800::bigint, actual_range_start), assert_equal(9223372036854775807::bigint, actual_range_end)
FROM _timescaledb_internal.dimension_calculate_default_range_open(9223372036854775806,10) AS res(actual_range_start, actual_range_end);



--closed
SELECT assert_equal((-9223372036854775808)::bigint, actual_range_start), assert_equal(1073741823::bigint, actual_range_end)
FROM _timescaledb_internal.dimension_calculate_default_range_closed(0,2::smallint) AS res(actual_range_start, actual_range_end);

SELECT assert_equal(1073741823::bigint, actual_range_start), assert_equal(9223372036854775807::bigint, actual_range_end)
FROM _timescaledb_internal.dimension_calculate_default_range_closed(1073741824,2::smallint) AS res(actual_range_start, actual_range_end);

SELECT assert_equal((-9223372036854775808)::bigint, actual_range_start), assert_equal(9223372036854775807::bigint, actual_range_end)
FROM _timescaledb_internal.dimension_calculate_default_range_closed(1073741824,1::smallint) AS res(actual_range_start, actual_range_end);
