/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * A file to define vector-const predicates for one type pair.
 */

#define PREDICATE_NAME GE
#define PREDICATE_EXPRESSION(X, Y) ((X) >= (Y))
#include "vector_const_one_predicate.c"

#define PREDICATE_NAME LE
#define PREDICATE_EXPRESSION(X, Y) ((X) <= (Y))
#include "vector_const_one_predicate.c"

#define PREDICATE_NAME LT
#define PREDICATE_EXPRESSION(X, Y) ((X) < (Y))
#include "vector_const_one_predicate.c"

#define PREDICATE_NAME GT
#define PREDICATE_EXPRESSION(X, Y) ((X) > (Y))
#include "vector_const_one_predicate.c"

#define PREDICATE_NAME EQ
#define PREDICATE_EXPRESSION(X, Y) ((X) == (Y))
#include "vector_const_one_predicate.c"

#define PREDICATE_NAME NE
#define PREDICATE_EXPRESSION(X, Y) ((X) != (Y))
#include "vector_const_one_predicate.c"

#undef VECTOR_CTYPE
#undef CONST_CTYPE
#undef CONST_CONVERSION
#undef PG_PREDICATE
