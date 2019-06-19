-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\unset ECHO
\o /dev/null
\ir include/test_utils.sql
\o
\set ECHO errors
\set VERBOSITY default

\o /dev/null

\c :TEST_DBNAME :ROLE_SUPERUSER
CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_calculate_default_range_open(
        dimension_value   BIGINT,
        interval_length   BIGINT,
    OUT range_start       BIGINT,
    OUT range_end         BIGINT)
    AS :MODULE_PATHNAME, 'ts_dimension_calculate_open_range_default' LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION _timescaledb_internal.dimension_calculate_default_range_closed(
        dimension_value   BIGINT,
        num_slices        SMALLINT,
    OUT range_start       BIGINT,
    OUT range_end         BIGINT)
    AS :MODULE_PATHNAME, 'ts_dimension_calculate_closed_range_default' LANGUAGE C STABLE;

\c :TEST_DBNAME :ROLE_DEFAULT_PERM_USER
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
