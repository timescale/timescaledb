/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Vector-const predicates for one pair of arithmetic types. For NaN comparison,
 * Postgres has its own nonstandard rules different from the IEEE floats.
 */

#define PREDICATE_NAME GE
#define PREDICATE_EXPRESSION(X, Y) (isnan((double) (X)) || (!isnan((double) (Y)) && (X) >= (Y)))
#include "pred_vector_const_arithmetic_single.c"

#define PREDICATE_NAME LE
#define PREDICATE_EXPRESSION(X, Y) (isnan((double) (Y)) || (!isnan((double) (X)) && (X) <= (Y)))
#include "pred_vector_const_arithmetic_single.c"

#define PREDICATE_NAME LT
#define PREDICATE_EXPRESSION(X, Y) (!isnan((double) (X)) && (isnan((double) (Y)) || (X) < (Y)))
#include "pred_vector_const_arithmetic_single.c"

#define PREDICATE_NAME GT
#define PREDICATE_EXPRESSION(X, Y) (!isnan((double) (Y)) && (isnan((double) (X)) || (X) > (Y)))
#include "pred_vector_const_arithmetic_single.c"

#define PREDICATE_NAME EQ
#define PREDICATE_EXPRESSION(X, Y) (isnan((double) (X)) ? isnan((double) (Y)) : ((X) == (Y)))
#include "pred_vector_const_arithmetic_single.c"

#define PREDICATE_NAME NE
#define PREDICATE_EXPRESSION(X, Y) (isnan((double) (X)) ? !isnan((double) (Y)) : ((X) != (Y)))
#include "pred_vector_const_arithmetic_single.c"

#undef VECTOR_CTYPE
#undef CONST_CTYPE
#undef CONST_CONVERSION
#undef PG_PREDICATE
