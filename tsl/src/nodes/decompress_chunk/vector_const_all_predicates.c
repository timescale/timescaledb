/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * A file to define all the vector predicates or to generate a dispatch
 * table for them.
 */

/* int8... functions. */
#define VECTOR_CTYPE int64
#define CONST_CTYPE int64
#if PG14_LT
/* No separate TIMESTAMPTZ functions in PG <= 13 */
#define PG_PREDICATE(X) F_INT8##X: case F_TIMESTAMP_##X
#else
#define PG_PREDICATE(X) F_INT8##X: case F_TIMESTAMPTZ_##X: case F_TIMESTAMP_##X
#endif
#define CONST_CONVERSION(X) DatumGetInt64(X)

#include "vector_const_one_type.c"

/* int4. functions. */
#define VECTOR_CTYPE int32
#define CONST_CTYPE int32
#define CONST_CONVERSION(X) DatumGetInt32(X)
#define PG_PREDICATE(X) F_INT4##X

#include "vector_const_one_type.c"

/* int24... functions. */
#define VECTOR_CTYPE int16
#define CONST_CTYPE int32
#define CONST_CONVERSION(X) DatumGetInt32(X)
#define PG_PREDICATE(X) F_INT24##X

#include "vector_const_one_type.c"

/* int84... functions. */
#define VECTOR_CTYPE int64
#define CONST_CTYPE int32
#define CONST_CONVERSION(X) DatumGetInt32(X)
#define PG_PREDICATE(X) F_INT84##X

#include "vector_const_one_type.c"

/* int48... functions. */
#define VECTOR_CTYPE int32
#define CONST_CTYPE int64
#define CONST_CONVERSION(X) DatumGetInt32(X)
#define PG_PREDICATE(X) F_INT48##X

#include "vector_const_one_type.c"

/* float8 functions. */
#define VECTOR_CTYPE float8
#define CONST_CTYPE float8
#define CONST_CONVERSION(X) DatumGetFloat8(X)
#define PG_PREDICATE(X) F_FLOAT8##X

#include "vector_const_one_type.c"

/* float4 functions. */
#define VECTOR_CTYPE float4
#define CONST_CTYPE float4
#define CONST_CONVERSION(X) DatumGetFloat4(X)
#define PG_PREDICATE(X) F_FLOAT4##X

#include "vector_const_one_type.c"
