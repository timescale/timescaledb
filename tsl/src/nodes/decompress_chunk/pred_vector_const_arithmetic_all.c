/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Define all supported "vector ? const" predicates for arithmetic types.
 */

/* In PG <= 13, in fmgroids.h the defines for TIMESTAMP functions specify
 * TIMESTAMPTZ oids, and the actual TIMESTAMP oids are nowhere to be found. Fix
 * this by manually defining the TIMESTAMPTZ defines to specify TIMESTAMP oids.
 * This means that the tz/non-tz versions are switched, but we don't care since
 * the implementation is the same.
 */
#if PG14_LT
#undef F_TIMESTAMPTZ_EQ
#undef F_TIMESTAMPTZ_NE
#undef F_TIMESTAMPTZ_LT
#undef F_TIMESTAMPTZ_LE
#undef F_TIMESTAMPTZ_GE
#undef F_TIMESTAMPTZ_GT
#define F_TIMESTAMPTZ_EQ 2052
#define F_TIMESTAMPTZ_NE 2053
#define F_TIMESTAMPTZ_LT 2054
#define F_TIMESTAMPTZ_LE 2055
#define F_TIMESTAMPTZ_GE 2056
#define F_TIMESTAMPTZ_GT 2057
#endif

/* int8 functions. */
#define VECTOR_CTYPE int64
#define CONST_CTYPE int64
#define CONST_CONVERSION(X) DatumGetInt64(X)
#define PG_PREDICATE(X) F_INT8##X: case F_TIMESTAMPTZ_##X: case F_TIMESTAMP_##X

#include "pred_vector_const_arithmetic_type_pair.c"

/* int4 functions. */
#define VECTOR_CTYPE int32
#define CONST_CTYPE int32
#define CONST_CONVERSION(X) DatumGetInt32(X)
#define PG_PREDICATE(X) F_INT4##X

#include "pred_vector_const_arithmetic_type_pair.c"

/* int24 functions. */
#define VECTOR_CTYPE int16
#define CONST_CTYPE int32
#define CONST_CONVERSION(X) DatumGetInt32(X)
#define PG_PREDICATE(X) F_INT24##X

#include "pred_vector_const_arithmetic_type_pair.c"

/* int84 functions. */
#define VECTOR_CTYPE int64
#define CONST_CTYPE int32
#define CONST_CONVERSION(X) DatumGetInt32(X)
#define PG_PREDICATE(X) F_INT84##X

#include "pred_vector_const_arithmetic_type_pair.c"

/* int48 functions. */
#define VECTOR_CTYPE int32
#define CONST_CTYPE int64
#define CONST_CONVERSION(X) DatumGetInt64(X)
#define PG_PREDICATE(X) F_INT48##X

#include "pred_vector_const_arithmetic_type_pair.c"

/* float8 functions. */
#define VECTOR_CTYPE float8
#define CONST_CTYPE float8
#define CONST_CONVERSION(X) DatumGetFloat8(X)
#define PG_PREDICATE(X) F_FLOAT8##X

#include "pred_vector_const_arithmetic_type_pair.c"

/* float4 functions. */
#define VECTOR_CTYPE float4
#define CONST_CTYPE float4
#define CONST_CONVERSION(X) DatumGetFloat4(X)
#define PG_PREDICATE(X) F_FLOAT4##X

#include "pred_vector_const_arithmetic_type_pair.c"
