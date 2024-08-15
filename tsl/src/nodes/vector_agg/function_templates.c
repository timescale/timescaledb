/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#define PG_AGG_OID(AGG_NAME, ARGUMENT_TYPE) F_##AGG_NAME##_##ARGUMENT_TYPE
#define PG_AGG_OID_HELPER(X, Y) PG_AGG_OID(X, Y)

#define FUNCTION_NAME_HELPER2(X, Y, Z) X##_##Y##_##Z
#define FUNCTION_NAME_HELPER(X, Y, Z) FUNCTION_NAME_HELPER2(X, Y, Z)
#define FUNCTION_NAME(Z) FUNCTION_NAME_HELPER(AGG_NAME, PG_TYPE, Z)

#define AGG_NAME MIN
#define PREDICATE(CURRENT, NEW) (NEW) < (CURRENT)

#include "minmax_arithmetic_types.c"

#undef PREDICATE
#undef AGG_NAME

#define AGG_NAME MAX
#define PREDICATE(CURRENT, NEW) (NEW) > (CURRENT)

#include "minmax_arithmetic_types.c"

#undef PREDICATE
#undef AGG_NAME

#undef FUNCTION_NAME
#undef FUNCTION_NAME_HELPER
#undef FUNCTION_NAME_HELPER2

#undef PG_AGG_OID_HELPER
#undef PG_AGG_OID
