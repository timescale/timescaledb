/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Vector-const predicates for one pair of arithmetic types.
 */

#define PREDICATE_NAME GE
#define PREDICATE_EXPRESSION(X, Y) ((X) >= (Y))
#include "pred_vector_const_arithmetic_single.c"

#define PREDICATE_NAME LE
#define PREDICATE_EXPRESSION(X, Y) ((X) <= (Y))
#include "pred_vector_const_arithmetic_single.c"

#define PREDICATE_NAME LT
#define PREDICATE_EXPRESSION(X, Y) ((X) < (Y))
#include "pred_vector_const_arithmetic_single.c"

#define PREDICATE_NAME GT
#define PREDICATE_EXPRESSION(X, Y) ((X) > (Y))
#include "pred_vector_const_arithmetic_single.c"

#define PREDICATE_NAME EQ
#define PREDICATE_EXPRESSION(X, Y) ((X) == (Y))
#include "pred_vector_const_arithmetic_single.c"

#define PREDICATE_NAME NE
#define PREDICATE_EXPRESSION(X, Y) ((X) != (Y))
#include "pred_vector_const_arithmetic_single.c"

#undef VECTOR_CTYPE
#undef CONST_CTYPE
#undef CONST_CONVERSION
#undef PG_PREDICATE
