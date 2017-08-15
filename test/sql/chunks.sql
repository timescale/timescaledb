\unset ECHO
\o /dev/null
\ir include/create_single_db.sql
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

--closed
SELECT assert_equal(0::bigint, actual_range_start), assert_equal(1073741823::bigint, actual_range_end)
FROM _timescaledb_internal.dimension_calculate_default_range_closed(0,2::smallint) AS res(actual_range_start, actual_range_end);

SELECT assert_equal(1073741823::bigint, actual_range_start), assert_equal(2147483647::bigint, actual_range_end)
FROM _timescaledb_internal.dimension_calculate_default_range_closed(1073741823,2::smallint) AS res(actual_range_start, actual_range_end);
