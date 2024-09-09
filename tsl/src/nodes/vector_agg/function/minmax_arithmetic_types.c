/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#define PG_TYPE INT2
#define CTYPE int16
#define DATUM_TO_CTYPE DatumGetInt16
#define CTYPE_TO_DATUM Int16GetDatum
#include "minmax_single.c"

#define PG_TYPE INT4
#define CTYPE int32
#define DATUM_TO_CTYPE DatumGetInt32
#define CTYPE_TO_DATUM Int32GetDatum
#include "minmax_single.c"

#define PG_TYPE INT8
#define CTYPE int64
#define DATUM_TO_CTYPE DatumGetInt64
#define CTYPE_TO_DATUM Int64GetDatum
#include "minmax_single.c"

#define PG_TYPE FLOAT4
#define CTYPE float
#define DATUM_TO_CTYPE DatumGetFloat4
#define CTYPE_TO_DATUM Float4GetDatum
#include "minmax_single.c"

#define PG_TYPE FLOAT8
#define CTYPE double
#define DATUM_TO_CTYPE DatumGetFloat8
#define CTYPE_TO_DATUM Float8GetDatum
#include "minmax_single.c"

#define PG_TYPE TIMESTAMP
#define CTYPE Timestamp
#define DATUM_TO_CTYPE DatumGetTimestamp
#define CTYPE_TO_DATUM TimestampGetDatum
#include "minmax_single.c"

#define PG_TYPE TIMESTAMPTZ
#define CTYPE TimestampTz
#define DATUM_TO_CTYPE DatumGetTimestampTz
#define CTYPE_TO_DATUM TimestampTzGetDatum
#include "minmax_single.c"

#undef PREDICATE
#undef AGG_NAME
