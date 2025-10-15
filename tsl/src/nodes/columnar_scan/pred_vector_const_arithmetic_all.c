/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Define all supported "vector ? const" predicates for arithmetic types.
 */

/* int8 functions. */
#define VECTOR_CTYPE int64
#define CONST_CTYPE int64
#define CONST_CONVERSION(X) DatumGetInt64(X)
#define PG_PREDICATE(X) F_INT8##X: case F_TIMESTAMPTZ_##X: case F_TIMESTAMP_##X

#include "pred_vector_const_arithmetic_type_pair.c"

/* int84 functions. */
#define VECTOR_CTYPE int64
#define CONST_CTYPE int32
#define CONST_CONVERSION(X) DatumGetInt32(X)
#define PG_PREDICATE(X) F_INT84##X

#include "pred_vector_const_arithmetic_type_pair.c"

/* int82 functions. */
#define VECTOR_CTYPE int64
#define CONST_CTYPE int16
#define CONST_CONVERSION(X) DatumGetInt16(X)
#define PG_PREDICATE(X) F_INT82##X

#include "pred_vector_const_arithmetic_type_pair.c"

/* int48 functions. */
#define VECTOR_CTYPE int32
#define CONST_CTYPE int64
#define CONST_CONVERSION(X) DatumGetInt64(X)
#define PG_PREDICATE(X) F_INT48##X

#include "pred_vector_const_arithmetic_type_pair.c"

/* int4 functions. */
#define VECTOR_CTYPE int32
#define CONST_CTYPE int32
#define CONST_CONVERSION(X) DatumGetInt32(X)
#define PG_PREDICATE(X) F_INT4##X

#include "pred_vector_const_arithmetic_type_pair.c"

/* int42 functions. */
#define VECTOR_CTYPE int32
#define CONST_CTYPE int16
#define CONST_CONVERSION(X) DatumGetInt16(X)
#define PG_PREDICATE(X) F_INT42##X

#include "pred_vector_const_arithmetic_type_pair.c"

/* int28 functions. */
#define VECTOR_CTYPE int16
#define CONST_CTYPE int64
#define CONST_CONVERSION(X) DatumGetInt64(X)
#define PG_PREDICATE(X) F_INT28##X

#include "pred_vector_const_arithmetic_type_pair.c"

/* int24 functions. */
#define VECTOR_CTYPE int16
#define CONST_CTYPE int32
#define CONST_CONVERSION(X) DatumGetInt32(X)
#define PG_PREDICATE(X) F_INT24##X

#include "pred_vector_const_arithmetic_type_pair.c"
/* int2 functions. */
#define VECTOR_CTYPE int16
#define CONST_CTYPE int16
#define CONST_CONVERSION(X) DatumGetInt16(X)
#define PG_PREDICATE(X) F_INT2##X

#include "pred_vector_const_arithmetic_type_pair.c"

/* float8 functions. */
#define VECTOR_CTYPE float8
#define CONST_CTYPE float8
#define CONST_CONVERSION(X) DatumGetFloat8(X)
#define PG_PREDICATE(X) F_FLOAT8##X

#include "pred_vector_const_arithmetic_type_pair.c"

/* float84 functions. */
#define VECTOR_CTYPE float8
#define CONST_CTYPE float4
#define CONST_CONVERSION(X) DatumGetFloat4(X)
#define PG_PREDICATE(X) F_FLOAT84##X

#include "pred_vector_const_arithmetic_type_pair.c"

/* float48 functions. */
#define VECTOR_CTYPE float4
#define CONST_CTYPE float8
#define CONST_CONVERSION(X) DatumGetFloat8(X)
#define PG_PREDICATE(X) F_FLOAT48##X

#include "pred_vector_const_arithmetic_type_pair.c"

/* float4 functions. */
#define VECTOR_CTYPE float4
#define CONST_CTYPE float4
#define CONST_CONVERSION(X) DatumGetFloat4(X)
#define PG_PREDICATE(X) F_FLOAT4##X

#include "pred_vector_const_arithmetic_type_pair.c"

/* date functions. */
#define VECTOR_CTYPE DateADT
#define CONST_CTYPE DateADT
#define CONST_CONVERSION(X) DatumGetDateADT(X)
#define PG_PREDICATE(X) F_DATE_##X

#include "pred_vector_const_arithmetic_type_pair.c"
